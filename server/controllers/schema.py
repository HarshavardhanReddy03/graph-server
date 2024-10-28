import os
import json
import networkx as nx
import logging
from ..config import SCHEMAARCHIVE_PATH, redis_client, LIVESCHEMA_PATH
from ..models.change import Change

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_live_schema():
    try:
        with open(f"{LIVESCHEMA_PATH}/current_schema.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"error": "Live schema not found"}


async def update_live_schema(update: Change):
    logger.info(f"Queueing schema update: {update}")
    update_data = update.dict()
    update_data["type"] = "schema"
    redis_client.rpush("changes", json.dumps(update_data))
    return {"status": "Schema update queued"}


async def queue_live_schema_update(update: Change):
    update_data = update.dict()
    update_data["type"] = "schema"
    redis_client.rpush("changes", json.dumps(update_data))
    return {"status": "Schema update queued"}
