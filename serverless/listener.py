import os, time, json, uuid, datetime, imaplib, email, re, io
from email.header import decode_header
from kafka import KafkaProducer
from dotenv import load_dotenv
import cloudinary
import cloudinary.uploader
from pymongo import MongoClient

load_dotenv()

IMAP_SERVER   = os.getenv("IMAP_SERVER", "imap.gmail.com")
IMAP_PORT     = int(os.getenv("IMAP_PORT", "993"))
EMAIL_ACCOUNT = os.getenv("EMAIL_USER")
APP_PASSWORD  = os.getenv("EMAIL_PASS")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "submission.events")

POLL_SECONDS = 10

# --- Cloudinary config
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET")
)

# --- MongoDB config
mongo = MongoClient(os.getenv("MONGO_URI"))
db = mongo["email_pipeline"]
emails_col = db["emails"]


# -------------------- Helpers --------------------

def _decode(s):
    if not s:
        return ""
    dh = decode_header(s)[0]
    text, enc = dh
    if isinstance(text, bytes):
        return text.decode(enc or "utf-8", errors="ignore")
    return text

def connect_imap():
    m = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    m.login(EMAIL_ACCOUNT, APP_PASSWORD)
    m.select("INBOX")
    return m

def safe_public_id(filename, processing_id):
    # Remove invalid chars
    base = re.sub(r'[^a-zA-Z0-9_.-]', '_', filename)
    return f"{processing_id}_{base}"

def upload_to_cloudinary(file_bytes, filename, processing_id, folder="emails"):
    file_stream = io.BytesIO(file_bytes)  # Cloudinary needs file-like object
    result = cloudinary.uploader.upload(
        file_stream,
        folder=folder,
        public_id=safe_public_id(filename, processing_id),
        resource_type="auto"
    )
    return {"url": result["secure_url"], "public_id": result["public_id"]}

def save_to_cloudinary(msg, processing_id):
    urls = {"raw_eml": None, "attachments": []}

    # Upload raw .eml file
    raw_bytes = msg.as_bytes()
    urls["raw_eml"] = upload_to_cloudinary(raw_bytes, f"{processing_id}_raw.eml", processing_id, folder="emails/raw")

    # Upload attachments
    if msg.is_multipart():
        for part in msg.walk():
            cdisp = (part.get("Content-Disposition") or "")
            if "attachment" in cdisp.lower():
                filename = part.get_filename() or f"{uuid.uuid4()}"
                file_bytes = part.get_payload(decode=True)
                uploaded = upload_to_cloudinary(file_bytes, filename, processing_id, folder="emails/attachments")
                urls["attachments"].append(uploaded)

    return urls

def build_event(from_addr, subject, attachment_count, cloudinary_refs, processing_id):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "submission.created",
        "processing_id": processing_id,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "source_service": "imap-intake",
        "payload": {
            "source": "imap",
            "metadata": {
                "from": from_addr,
                "subject": subject,
                "attachment_count": attachment_count,
            },
            "cloudinary_refs": cloudinary_refs
        },
        "trace_id": str(uuid.uuid4())
    }

def parse_message(msg):
    subject = _decode(msg.get("Subject"))
    from_   = _decode(msg.get("From"))

    attach_count = 0
    if msg.is_multipart():
        for part in msg.walk():
            cdisp = (part.get("Content-Disposition") or "")
            if "attachment" in cdisp.lower():
                attach_count += 1

    return from_, subject, attach_count


# -------------------- Main Loop --------------------

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all",
    )

    imap = None
    print(f"✅ IMAP→Kafka started. Polling every {POLL_SECONDS}s. Topic: {KAFKA_TOPIC}")

    while True:
        try:
            if imap is None:
                imap = connect_imap()

            status, data = imap.search(None, "UNSEEN")
            if status != "OK":
                time.sleep(POLL_SECONDS)
                continue

            ids = data[0].split()
            if not ids:
                print("📭 No new unseen emails")
                time.sleep(POLL_SECONDS)
                continue

            latest_id = ids[-1]
            status, msg_data = imap.fetch(latest_id, "(RFC822)")
            if status != "OK" or not msg_data or not isinstance(msg_data[0], tuple):
                print("⚠ Failed to fetch latest unseen email payload")
                time.sleep(POLL_SECONDS)
                continue

            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            from_, subject, attach_count = parse_message(msg)
            processing_id = str(uuid.uuid4())

            # Upload to Cloudinary
            cloudinary_refs = save_to_cloudinary(msg, processing_id)

            # Build event
            event = build_event(from_, subject, attach_count, cloudinary_refs, processing_id)

            # Save to Mongo
            doc = {
                "event_id": event["event_id"],
                "processing_id": event["processing_id"],
                "from": from_,
                "subject": subject,
                "timestamp": event["timestamp"],
                "status": "processed",
                "cloudinary_refs": cloudinary_refs,
                "metadata": event["payload"]["metadata"],
                "trace": {
                    "source_service": event["source_service"],
                    "trace_id": event["trace_id"]
                }
            }
            emails_col.insert_one(doc)

            # Send event to Kafka
            producer.send(KAFKA_TOPIC, value=event, key=event["processing_id"].encode("utf-8"))
            producer.flush()

            print(f"📩 Sent to Kafka & Mongo → from: {from_} | subject: {subject} | attachments: {attach_count}")

            imap.store(latest_id, "+FLAGS", r"(\Seen)")
            time.sleep(POLL_SECONDS)

        except imaplib.IMAP4.abort as e:
            print(f"🔌 IMAP abort, reconnecting… ({e})")
            try:
                if imap:
                    imap.logout()
            except Exception:
                pass
            imap = None
            time.sleep(3)

        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(POLL_SECONDS)


if __name__ == "_main_":
    main()