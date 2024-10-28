from typing import Dict, List, Any
import json


def compress_graph_json(data: Dict) -> Dict:
    compressed = {
        "metadata": {"node_types": {}, "keys": {}},
        "data": {"nodes": {}, "links": []},
    }

    # Compress nodes
    for node_type, nodes in data.get("nodes", {}).items():
        if not nodes:
            continue

        # Get template of keys from first node
        template_keys = list(next(iter(nodes.values())).keys())
        compressed["metadata"]["node_types"][node_type] = template_keys

        # Convert nodes to array format
        node_arrays = []
        for node_id, node_data in nodes.items():
            node_values = [node_data[key] for key in template_keys]
            node_arrays.append([node_id] + node_values)

        compressed["data"]["nodes"][node_type] = node_arrays

    # Compress links
    if "links" in data:
        link_template = ["source", "target", "key"]
        compressed["metadata"]["keys"]["links"] = link_template
        compressed["data"]["links"] = [
            [link[key] for key in link_template] for link in data["links"]
        ]

    return compressed


def decompress_graph_json(compressed_data: Dict) -> Dict:
    decompressed = {"nodes": {}, "links": []}

    # Decompress nodes
    for node_type, template_keys in compressed_data["metadata"]["node_types"].items():
        decompressed["nodes"][node_type] = {}

        for node_array in compressed_data["data"]["nodes"].get(node_type, []):
            node_id = node_array[0]
            node_data = dict(zip(template_keys, node_array[1:]))
            decompressed["nodes"][node_type][node_id] = node_data

    # Decompress links
    link_template = compressed_data["metadata"]["keys"]["links"]
    decompressed["links"] = [
        dict(zip(link_template, link_data))
        for link_data in compressed_data["data"]["links"]
    ]

    return decompressed
