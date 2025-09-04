from redis import Redis
import os
from dotenv import load_dotenv

load_dotenv()

config = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
}

class RedisCache:
    _redis = None
    
    @staticmethod
    def get_client(self):
        if not self._redis:
            self._redis = Redis(config)
        
        return self._redis
    
rd = RedisCache.get_client()