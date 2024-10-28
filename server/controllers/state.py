import os
import json
import networkx as nx
from ..config import LIVESTATE_PATH, STATEARCHIVE_PATH, redis_client
from ..models.change import Change


async def get_live_state():
    try:
        with open(f"{LIVESTATE_PATH}/current_state.json", "r") as f:
            state_data = json.load(f)
            return state_data if state_data else {"nodes": {}, "links": []}
    except (FileNotFoundError, json.JSONDecodeError):
        # Return empty state structure if file doesn't exist or is invalid
        return {"nodes": {}, "links": []}


async def queue_live_state_update(update: Change):
    update_data = update.dict()
    update_data["type"] = "state"
    redis_client.rpush("changes", json.dumps(update_data))
    return {"status": "State update queued"}
