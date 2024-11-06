import networkx as nx
import logging
from copy import deepcopy
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def process_schema_create(
    payload: Dict[str, Any], schema_data: nx.DiGraph
) -> nx.DiGraph:
    """
    Process schema creation payload and update the schema graph.
    Handles both node and edge creation operations.

    Node payload structure:
    {
        "node_id": str,
        "node_type": str,
        "properties": {
            "name": str,
            "description": str,
            "attributes": Dict[str, Any],
            ...
        }
    }

    Edge payload structure:
    {
        "source_id": str,
        "target_id": str,
        "edge_type": str,
        "properties": Dict[str, Any]
    }
    """
    try:
        schema = deepcopy(schema_data)

        # Check if this is an edge creation
        if "source_id" in payload and "target_id" in payload:
            source_id = payload["source_id"]
            target_id = payload["target_id"]

            # Verify both nodes exist
            if not schema.has_node(source_id):
                raise ValueError(f"Source node {source_id} does not exist in schema")
            if not schema.has_node(target_id):
                raise ValueError(f"Target node {target_id} does not exist in schema")

            logger.info(f"Processing edge create: {source_id} -> {target_id}")

            # Add the edge with its properties
            schema.add_edge(
                source_id,
                target_id,
                relationship_type=payload["edge_type"],
                **payload.get("properties", {}),
            )

            logger.info(f"Edge create complete: {source_id} -> {target_id}")

        # Node creation
        else:
            node_id = payload["node_id"]
            logger.info(f"Processing node create: {node_id}")

            # Verify node doesn't already exist
            if schema.has_node(node_id):
                raise ValueError(f"Node {node_id} already exists in schema")

            # Add node with its properties
            schema.add_node(
                node_id, node_type=payload["node_type"], **payload["properties"]
            )

            logger.info(f"Node create complete for {node_id}")

        return schema

    except KeyError as e:
        logger.error(f"Missing required field in create payload: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing schema create: {str(e)}")
        raise


def process_schema_update(
    payload: Dict[str, Any], schema_data: nx.DiGraph
) -> nx.DiGraph:
    """
    Process schema update payload and update the schema graph.

    Expected payload structure:
    {
        "node_id": str,
        "updates": {
            "properties": {
                "name": str,
                "description": str,
                "attributes": Dict[str, Any],
                ...
            }
        }
    }

    Edge updates are handled through separate create/delete operations
    """
    try:
        schema = deepcopy(schema_data)
        node_id = payload["node_id"]

        logger.info(f"Processing schema update: {node_id}")

        if not schema.has_node(node_id):
            raise ValueError(f"Node {node_id} does not exist in schema")

        # Update node properties
        if "properties" in payload["updates"]:
            for key, value in payload["updates"]["properties"].items():
                schema.nodes[node_id][key] = value

        logger.info(f"Update complete for {node_id}")
        return schema

    except KeyError as e:
        logger.error(f"Missing required field in update payload: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing schema update: {str(e)}")
        raise


def process_schema_delete(
    payload: Dict[str, Any], schema_data: nx.DiGraph
) -> nx.DiGraph:
    """
    Process schema deletion payload and update the schema graph.
    For edge deletion, specify both source_id and target_id.
    For node deletion, specify node_id.

    Node deletion payload:
    {
        "node_id": str,
        "cascade": bool  # Whether to delete connected nodes
    }

    Edge deletion payload:
    {
        "source_id": str,
        "target_id": str,
        "edge_type": str  # Optional, if specified will only delete edges of this type
    }
    """
    try:
        schema = deepcopy(schema_data)

        # Check if this is an edge deletion
        if "source_id" in payload and "target_id" in payload:
            source_id = payload["source_id"]
            target_id = payload["target_id"]

            logger.info(f"Processing edge delete: {source_id} -> {target_id}")

            if not schema.has_edge(source_id, target_id):
                raise ValueError(f"Edge from {source_id} to {target_id} does not exist")

            # If edge_type is specified, only delete edges of that type
            if "edge_type" in payload:
                edge_data = schema.get_edge_data(source_id, target_id)
                if edge_data.get("relationship_type") == payload["edge_type"]:
                    schema.remove_edge(source_id, target_id)
            else:
                schema.remove_edge(source_id, target_id)

            logger.info(f"Edge delete complete: {source_id} -> {target_id}")

        # Node deletion
        else:
            node_id = payload["node_id"]
            logger.info(f"Processing node delete: {node_id}")

            if not schema.has_node(node_id):
                raise ValueError(f"Node {node_id} does not exist in schema")

            if payload.get("cascade", False):
                # Get all descendant nodes
                descendants = nx.descendants(schema, node_id)
                # Remove all descendants
                schema.remove_nodes_from(descendants)

            # Remove the target node and all its edges
            schema.remove_node(node_id)

            logger.info(f"Node delete complete: {node_id}")

        return schema

    except KeyError as e:
        logger.error(f"Missing required field in delete payload: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing schema delete: {str(e)}")
        raise
