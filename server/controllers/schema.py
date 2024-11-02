import os
import json
import networkx as nx
import logging
from ..config import get_paths, redis_client
from ..models.change import Change

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_live_schema(version: str = None):
    paths = get_paths(version)
    try:
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"error": "Live schema not found"}


async def update_live_schema(update: Change, version: str = None):
    logger.info(f"Queueing schema update for version: {version}")

    # Ensure version directories exist
    paths = get_paths(version)
    os.makedirs(paths["LIVESCHEMA_PATH"], exist_ok=True)

    # Create empty schema file if it doesn't exist
    schema_file = f"{paths['LIVESCHEMA_PATH']}/current_schema.json"
    if not os.path.exists(schema_file):
        with open(schema_file, "w") as f:
            json.dump({"nodes": {}, "links": []}, f)

    # Queue the update
    update_data = update.dict()
    update_data["type"] = "schema"
    update_data["version"] = version
    redis_client.rpush("changes", json.dumps(update_data))
    return {"status": "Schema update queued"}


async def queue_live_schema_update(update: Change):
    update_data = update.dict()
    update_data["type"] = "schema"

    # Ensure version directories exist
    paths = get_paths(update_data.get("version"))
    os.makedirs(paths["LIVESCHEMA_PATH"], exist_ok=True)

    # Create empty schema file if it doesn't exist
    schema_file = f"{paths['LIVESCHEMA_PATH']}/current_schema.json"
    if not os.path.exists(schema_file):
        with open(schema_file, "w") as f:
            json.dump({"nodes": {}, "links": []}, f)

    redis_client.rpush("changes", json.dumps(update_data))
    return {"status": "Schema update queued"}
