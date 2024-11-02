import os
import json
import logging
from ..config import get_paths, redis_client
from ..models.change import Change

logger = logging.getLogger(__name__)


async def get_live_state(version: str = None):
    paths = get_paths(version)
    os.makedirs(paths["LIVESTATE_PATH"], exist_ok=True)

    state_file = f"{paths['LIVESTATE_PATH']}/current_state.json"
    if not os.path.exists(state_file):
        with open(state_file, "w") as f:
            json.dump({"nodes": {}, "links": []}, f)

    try:
        with open(state_file, "r") as f:
            state_data = json.load(f)
            return state_data if state_data else {"nodes": {}, "links": []}
    except (FileNotFoundError, json.JSONDecodeError):
        return {"nodes": {}, "links": []}


async def queue_live_state_update(update: Change, version: str = None):
    update_data = update.dict()
    update_data["type"] = "state"
    update_data["version"] = version

    # Ensure version directories exist
    paths = get_paths(version)
    os.makedirs(paths["LIVESTATE_PATH"], exist_ok=True)

    # Create empty state file if it doesn't exist
    state_file = f"{paths['LIVESTATE_PATH']}/current_state.json"
    if not os.path.exists(state_file):
        with open(state_file, "w") as f:
            json.dump({"nodes": {}, "links": []}, f)

    redis_client.rpush("changes", json.dumps(update_data))
    return {"status": "State update queued"}
