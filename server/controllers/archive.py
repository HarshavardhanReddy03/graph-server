import os
import json
import networkx as nx
from ..config import (
    NETWORKOBJECT_PATH,
    GRAPHOBJECT_PATH,
    STATEARCHIVE_PATH,
    SCHEMAARCHIVE_PATH,
)


async def get_schema_archive_list():
    archives = os.listdir(SCHEMAARCHIVE_PATH)
    return [
        int(f.split("_")[1].split(".")[0]) for f in archives if f.startswith("schema_")
    ]


async def get_specific_schema_archive(timestamp: int):
    file_path = os.path.join(SCHEMAARCHIVE_PATH, f"schema_{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "Schema archive not found"}


async def get_state_archive_list():
    archives = os.listdir(STATEARCHIVE_PATH)
    return [
        int(f.split("_")[1].split(".")[0]) for f in archives if f.startswith("state_")
    ]


async def get_specific_state_archive(timestamp: int):
    file_path = os.path.join(STATEARCHIVE_PATH, f"state_{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "State archive not found"}


async def get_network_object_timestamps():
    files = os.listdir(NETWORKOBJECT_PATH)
    return [int(f.split("_")[1].split(".")[0]) for f in files if f.endswith(".json")]


async def get_network_object(timestamp: int):
    file_path = os.path.join(NETWORKOBJECT_PATH, f"network_{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "Network object not found"}


async def get_geometric_object_timestamps():
    files = os.listdir(GRAPHOBJECT_PATH)
    return [int(f.split("_")[1].split(".")[0]) for f in files if f.endswith(".graphml")]


async def get_geometric_object(timestamp: int):
    file_path = os.path.join(GRAPHOBJECT_PATH, f"graph_{timestamp}.graphml")
    if os.path.exists(file_path):
        G = nx.read_graphml(file_path)
        return nx.node_link_data(G)
    else:
        return {"error": "Geometric object not found"}
