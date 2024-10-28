import os
import redis
import psycopg2

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
POSTGRES_URL = os.environ.get(
    "POSTGRES_URL", "postgresql://user:password@localhost:5432/deltachanges"
)
LIVESTATE_PATH = os.environ.get("LIVESTATE_PATH", "/app/data/livestate")
STATEARCHIVE_PATH = os.environ.get("STATEARCHIVE_PATH", "/app/data/statearchive")
NETWORKOBJECT_PATH = os.environ.get("NETWORKOBJECT_PATH", "/app/data/networkobjects")
GRAPHOBJECT_PATH = os.environ.get("GRAPHOBJECT_PATH", "/app/data/graphobjects")
SCHEMAARCHIVE_PATH = os.environ.get("SCHEMAARCHIVE_PATH", "/app/data/schemaarchive")
LIVESCHEMA_PATH = os.environ.get("LIVESCHEMA_PATH", "/app/data/liveschema")

redis_client = redis.Redis.from_url(REDIS_URL)
postgres_conn = psycopg2.connect(POSTGRES_URL)
