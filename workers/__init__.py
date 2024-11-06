import networkx as nx
from networkx.readwrite import json_graph
import threading
import json
from time import sleep
import logging
import uuid
import time
import os

from .actions import process_schema_create, process_schema_update, process_schema_delete

from utils.compression import compress_graph_json, decompress_graph_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from server.config import (
    get_paths,
    redis_client,
    postgres_conn,
)

# Get default paths
paths = get_paths()
LIVESTATE_PATH = paths["LIVESTATE_PATH"]
STATEARCHIVE_PATH = paths["STATEARCHIVE_PATH"]
SCHEMAARCHIVE_PATH = paths["SCHEMAARCHIVE_PATH"]
LIVESCHEMA_PATH = paths["LIVESCHEMA_PATH"]

CURRENT_TIMESTAMP = None


def write_to_postgres(timestamp, change_data=None):
    try:
        cursor = postgres_conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS state_deltas (
                timestamp BIGINT PRIMARY KEY,
                action VARCHAR(50),
                change_type VARCHAR(50),
                change_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert change delta
        if change_data:
            cursor.execute(
                """
                INSERT INTO state_deltas 
                (timestamp, action, change_type, change_data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (timestamp) DO NOTHING
            """,
                (
                    timestamp,
                    change_data["action"],
                    change_data["type"],
                    json.dumps(change_data["data"]),
                ),
            )

        postgres_conn.commit()
        cursor.close()
        logger.info(f"Successfully wrote delta for timestamp {timestamp}")

    except Exception as e:
        logger.error(f"Error writing delta to postgres: {str(e)}")
        postgres_conn.rollback()


def main_worker():
    logger.info("Starting main worker")
    while True:
        # Check Redis for new changes
        latest_change = redis_client.lpop("changes")
        if latest_change:
            change_data = json.loads(latest_change)
            version = change_data.get("version")

            if not version:
                logger.warning("No version specified in change data, using default")
                version = "default"

            logger.info(
                f"Processing change type: {change_data['type']} action: {change_data['action']} version: {version}"
            )

            # Get versioned paths for this change
            paths = get_paths(version)

            if change_data["type"] == "state":
                # process_state_change(change_data, paths)
                pass
            elif change_data["type"] == "schema":
                process_schema_change(change_data, paths)
        else:
            sleep(0.01)  # Wait before checking again


def process_state_change(change_data, paths):
    logger.info(f"Processing state change action: {change_data['action']}")
    try:
        # Read current state, initialize from schema only if it doesn't exist
        try:
            with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "r") as f:
                state_data = json.load(f)
                if isinstance(state_data, str):
                    state_data = json.loads(state_data)
        except (FileNotFoundError, json.JSONDecodeError):
            # Initialize new state from schema
            with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
                schema_data = json.load(f)
            state_data = {
                "nodes": {
                    node_type: nodes.copy()
                    for node_type, nodes in schema_data.get("nodes", {}).items()
                },
                "links": schema_data.get("links", []).copy(),
            }

        if not state_data or not isinstance(state_data, dict):
            state_data = {"nodes": {}, "links": []}

        # Process the state change
        if change_data["action"] in ["Create", "Update"]:
            for node_type, nodes in change_data["data"]["nodes"].items():
                # Ensure node type container exists
                if node_type not in state_data["nodes"]:
                    state_data["nodes"][node_type] = {}
                # Update nodes (both instances and regular nodes)
                state_data["nodes"][node_type].update(nodes)

            # Update links
            if "links" in change_data["data"]:
                new_links = [
                    link
                    for link in change_data["data"]["links"]
                    if link not in state_data["links"]
                ]
                state_data["links"].extend(new_links)

        # Save and archive state
        with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "w") as f:
            json.dump(state_data, f, indent=2)

        with open(
            f"{paths['STATEARCHIVE_PATH']}/state_{change_data['timestamp']}.json", "w"
        ) as f:
            json.dump(state_data, f, indent=2)

        write_to_postgres(change_data["timestamp"], change_data)
        return True

    except Exception as e:
        logger.error(f"Error processing state change: {str(e)}")
        return False


def create_initial_schema_and_state(paths):
    schema_data = nx.DiGraph()
    schema_node_link_data = json_graph.node_link_data(schema_data)

    state_data = nx.DiGraph()
    state_node_link_data = json_graph.node_link_data(state_data)

    # Create initial files if they don't exist
    with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "w") as f:
        json.dump(schema_node_link_data, f, indent=2)
    with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "w") as f:
        json.dump(state_node_link_data, f, indent=2)


def load_live_schema(paths):
    try:
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "r") as f:
            schema_node_link_data = json.load(f)
            return json_graph.node_link_graph(schema_node_link_data)
    except (FileNotFoundError, json.JSONDecodeError):
        create_initial_schema_and_state(paths)
        return load_live_schema(paths)


def load_live_state(paths):
    try:
        with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "r") as f:
            state_node_link_data = json.load(f)
            return json_graph.node_link_graph(state_node_link_data)
    except (FileNotFoundError, json.JSONDecodeError):
        create_initial_schema_and_state(paths)
        return load_live_state(paths)


def save_live_schema(schema_data, paths):
    try:
        os.makedirs(paths["LIVESCHEMA_PATH"], exist_ok=True)
        with open(f"{paths['LIVESCHEMA_PATH']}/current_schema.json", "w") as f:
            schema_node_link_data = json_graph.node_link_data(schema_data)
            json.dump(schema_node_link_data, f, indent=2)
    except (FileNotFoundError, json.JSONDecodeError):
        create_initial_schema_and_state(paths)
        save_live_schema(schema_data, paths)


def save_live_state(state_data, paths):
    try:
        os.makedirs(paths["LIVESTATE_PATH"], exist_ok=True)
        with open(f"{paths['LIVESTATE_PATH']}/current_state.json", "w") as f:
            state_node_link_data = json_graph.node_link_data(state_data)
            json.dump(state_node_link_data, f, indent=2)
    except (FileNotFoundError, json.JSONDecodeError):
        create_initial_schema_and_state(paths)
        save_live_state(state_data, paths)


def save_archive_schema(schema_data, paths, timestamp):
    try:
        os.makedirs(paths["SCHEMAARCHIVE_PATH"], exist_ok=True)
        with open(f"{paths['SCHEMAARCHIVE_PATH']}/{timestamp}.json", "w") as f:
            schema_node_link_data = json_graph.node_link_data(schema_data)
            compressed_schema = compress_graph_json(schema_node_link_data)
            json.dump(compressed_schema, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving archive schema: {str(e)}")


def save_archive_state(state_data, paths, timestamp):
    try:
        os.makedirs(paths["STATEARCHIVE_PATH"], exist_ok=True)
        with open(f"{paths['STATEARCHIVE_PATH']}/{timestamp}.json", "w") as f:
            state_node_link_data = json_graph.node_link_data(state_data)
            compressed_state = compress_graph_json(state_node_link_data)
            json.dump(compressed_state, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving archive state: {str(e)}")


def process_schema_change(change_data, paths):
    global CURRENT_TIMESTAMP
    try:
        schema_data = load_live_schema(paths)
        state_data = load_live_state(paths)

        if change_data["action"] in ["create", "update", "delete"]:

            if change_data["action"] == "create":
                schema_data = process_schema_create(change_data["payload"], schema_data)
            elif change_data["action"] == "update":
                schema_data = process_schema_update(change_data["payload"], schema_data)
            elif change_data["action"] == "delete":
                schema_data = process_schema_delete(change_data["payload"], schema_data)

        save_live_schema(schema_data, paths)
        save_live_state(state_data, paths)

        logger.info(
            f"Current timestamp: {CURRENT_TIMESTAMP}, change timestamp: {change_data['timestamp']}"
        )

        if CURRENT_TIMESTAMP is None:
            CURRENT_TIMESTAMP = change_data["timestamp"]

        if change_data["timestamp"] != CURRENT_TIMESTAMP:
            timestamp = change_data["timestamp"]
            save_archive_schema(schema_data, paths, timestamp)
            save_archive_state(state_data, paths, timestamp)
            CURRENT_TIMESTAMP = timestamp

        return True

    except Exception as e:
        logger.error(f"Error processing schema change: {str(e)}")
        return False


# Function to start the worker thread
def start_worker():
    worker_thread = threading.Thread(target=main_worker, daemon=True)
    worker_thread.start()


def generate_instance_id(parent_id: str, instance_number: int) -> str:
    return f"{parent_id}-i{instance_number}"


if __name__ == "__main__":
    start_worker()
