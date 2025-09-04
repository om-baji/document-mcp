import cloudinary
from threading import Lock
import os
import cloudinary.uploader
import cloudinary.api
from dotenv import load_dotenv

load_dotenv()

class CloudinarySingleton:
    _instance = None
    _lock = Lock()

    def __new__(cls, config=None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(CloudinarySingleton, cls).__new__(cls)
                if config:
                    cloudinary.config(**config)
            return cls._instance

    def upload(self, file, **options):
        return cloudinary.uploader.upload(file, **options)

    def get(self, public_id, **options):
        return cloudinary.api.resource(public_id, **options)

config = {
    'cloud_name': os.getenv("CLOUD_NAME"),
    'api_key': os.getenv("CLOUD_API_KEY"),
    'api_secret': os.getenv("CLOUD_API_SECRET")
}

cloud = CloudinarySingleton(config)