from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

config = {
    "host": os.getenv("MONGO_HOST", "localhost"),
    "port": int(os.getenv("MONGO_PORT", 27017)),
    "username": os.getenv("MONGO_USER"),
    "password": os.getenv("MONGO_PASS"),
}

class MongoDBClient:
    _client = None

    @classmethod
    def get_client(cls):
        if cls._client is None:
            uri = f"mongodb://{config['host']}:{config['port']}"
            if config["username"] and config["password"]:
                uri = (
                    f"mongodb://{config['username']}:{config['password']}@"
                    f"{config['host']}:{config['port']}/?authSource={config['authSource']}"
                )
            cls._client = MongoClient(uri)
        return cls._client