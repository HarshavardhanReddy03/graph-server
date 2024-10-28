import networkx as nx
import threading
import json
from time import sleep
import logging
import uuid
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from server.config import (
    LIVESCHEMA_PATH,
    SCHEMAARCHIVE_PATH,
    redis_client,
    postgres_conn,
    LIVESTATE_PATH,
    STATEARCHIVE_PATH,
)


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
            logger.info(
                f"Processing change type: {change_data['type']} action: {change_data['action']}"
            )
            if change_data["type"] == "state":
                process_state_change(change_data)
            elif change_data["type"] == "schema":
                process_schema_change(change_data)
        else:
            sleep(0.01)  # Wait before checking again


def process_state_change(change_data):
    logger.info(f"Processing state change action: {change_data['action']}")
    try:
        # Read current state, initialize from schema only if it doesn't exist
        try:
            with open(f"{LIVESTATE_PATH}/current_state.json", "r") as f:
                state_data = json.load(f)
                if isinstance(state_data, str):
                    state_data = json.loads(state_data)
        except (FileNotFoundError, json.JSONDecodeError):
            # Initialize new state from schema
            with open(f"{LIVESCHEMA_PATH}/current_schema.json", "r") as f:
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
        with open(f"{LIVESTATE_PATH}/current_state.json", "w") as f:
            json.dump(state_data, f, indent=2)

        with open(
            f"{STATEARCHIVE_PATH}/state_{change_data['timestamp']}.json", "w"
        ) as f:
            json.dump(state_data, f, indent=2)

        write_to_postgres(change_data["timestamp"], change_data)
        return True

    except Exception as e:
        logger.error(f"Error processing state change: {str(e)}")
        return False


def process_schema_change(change_data):
    try:
        # Read current schema and state
        try:
            with open(f"{LIVESCHEMA_PATH}/current_schema.json", "r") as f:
                schema_data = json.load(f)
            with open(f"{LIVESTATE_PATH}/current_state.json", "r") as f:
                state_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            schema_data = {"nodes": {}, "links": []}
            state_data = {"nodes": {}, "links": []}

        if change_data["action"] in ["Create", "Update"]:
            # Process nodes
            for node_type, nodes in change_data["data"]["nodes"].items():
                for node_id, node_data in nodes.items():
                    # Update schema
                    if node_type not in schema_data["nodes"]:
                        schema_data["nodes"][node_type] = {}
                    schema_data["nodes"][node_type][node_id] = node_data

                    # Create instances for nodes with units_in_chain
                    if "units_in_chain" in node_data:
                        units = node_data["units_in_chain"]
                        instance_type = f"{node_type}Instance"

                        if instance_type not in state_data["nodes"]:
                            state_data["nodes"][instance_type] = {}

                        # Create or update instances
                        current_instances = [
                            inst
                            for inst in state_data["nodes"][instance_type].values()
                            if inst["parent_id"] == node_id
                        ]

                        # Remove excess instances if units decreased
                        if len(current_instances) > units:
                            for instance in current_instances[units:]:
                                del state_data["nodes"][instance_type][instance["id"]]

                        # Add new instances if units increased
                        while len(current_instances) < units:
                            instance = {
                                "id": str(uuid.uuid4()),
                                "parent_id": node_id,
                                "status": "available",
                                "created_at": int(time.time()),
                                "updated_at": int(time.time()),
                            }
                            state_data["nodes"][instance_type][
                                instance["id"]
                            ] = instance
                            current_instances.append(instance)

            # Process links
            if "links" in change_data["data"]:
                for link in change_data["data"]["links"]:
                    if link not in schema_data["links"]:
                        schema_data["links"].append(link)

        # Save updated schema and state
        with open(f"{LIVESCHEMA_PATH}/current_schema.json", "w") as f:
            json.dump(schema_data, f, indent=2)
        with open(f"{LIVESTATE_PATH}/current_state.json", "w") as f:
            json.dump(state_data, f, indent=2)

        # Archive both
        timestamp = change_data["timestamp"]
        with open(f"{SCHEMAARCHIVE_PATH}/schema_{timestamp}.json", "w") as f:
            json.dump(schema_data, f, indent=2)
        with open(f"{STATEARCHIVE_PATH}/state_{timestamp}.json", "w") as f:
            json.dump(state_data, f, indent=2)

        return True

    except Exception as e:
        logger.error(f"Error processing schema change: {str(e)}")
        return False


# Function to start the worker thread
def start_worker():
    worker_thread = threading.Thread(target=main_worker, daemon=True)
    worker_thread.start()


if __name__ == "__main__":
    start_worker()
