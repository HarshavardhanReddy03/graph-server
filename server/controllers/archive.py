import os
import json
import networkx as nx
from ..config import get_paths


async def get_schema_archive_list(version: str = None):
    paths = get_paths(version)
    archives = os.listdir(paths["SCHEMAARCHIVE_PATH"])
    return [int(f.split(".")[0]) for f in archives]


async def get_specific_schema_archive(timestamp: int, version: str = None):
    paths = get_paths(version)
    file_path = os.path.join(paths["SCHEMAARCHIVE_PATH"], f"{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "Schema archive not found"}


async def get_state_archive_list(version: str = None):
    paths = get_paths(version)
    archives = os.listdir(paths["STATEARCHIVE_PATH"])
    return [int(f.split(".")[0]) for f in archives]


async def get_specific_state_archive(timestamp: int, version: str = None):
    paths = get_paths(version)
    file_path = os.path.join(paths["STATEARCHIVE_PATH"], f"state_{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "State archive not found"}


async def get_network_object_timestamps(version: str = None):
    paths = get_paths(version)
    files = os.listdir(paths["NETWORKOBJECT_PATH"])
    return [int(f.split("_")[1].split(".")[0]) for f in files if f.endswith(".json")]


async def get_network_object(timestamp: int, version: str = None):
    paths = get_paths(version)
    file_path = os.path.join(paths["NETWORKOBJECT_PATH"], f"network_{timestamp}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {"error": "Network object not found"}


async def get_geometric_object_timestamps(version: str = None):
    paths = get_paths(version)
    files = os.listdir(paths["GRAPHOBJECT_PATH"])
    return [int(f.split("_")[1].split(".")[0]) for f in files if f.endswith(".graphml")]


async def get_geometric_object(timestamp: int, version: str = None):
    paths = get_paths(version)
    file_path = os.path.join(paths["GRAPHOBJECT_PATH"], f"graph_{timestamp}.graphml")
    if os.path.exists(file_path):
        G = nx.read_graphml(file_path)
        return nx.node_link_data(G)
    else:
        return {"error": "Geometric object not found"}
