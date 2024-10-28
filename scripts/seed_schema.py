import json
import random
import requests
import time
import uuid
from typing import Dict, List


def generate_business_unit() -> Dict:
    return {
        "id": str(uuid.uuid4()),
        "name": f"BU_{random.randint(1000, 9999)}",
        "description": f"Business Unit {random.randint(1, 5)}",
        "revenue": round(random.uniform(1000000, 5000000), 2),
        "node_type": "BusinessUnit",
    }


def generate_product_family() -> Dict:
    return {
        "id": str(uuid.uuid4()),
        "name": f"PF_{random.randint(1000, 9999)}",
        "revenue": round(random.uniform(100000, 1000000), 2),
        "node_type": "ProductFamily",
    }


def generate_product_offering() -> Dict:
    return {
        "id": str(uuid.uuid4()),
        "name": f"PO_{random.randint(1000, 9999)}",
        "cost": round(random.uniform(1000, 10000), 2),
        "demand": random.randint(100, 1000),
        "node_type": "ProductOffering",
    }


def generate_facility() -> Dict:
    return {
        "id": str(uuid.uuid4()),
        "name": f"Facility_{random.randint(1000, 9999)}",
        "type": random.choice(["Assembly", "Manufacturing", "Distribution"]),
        "location": f"Location_{random.randint(1, 5)}",
        "max_capacity": random.randint(5000, 10000),
        "operating_cost": round(random.uniform(10000, 50000), 2),
        "node_type": "Facility",
    }


def generate_supplier() -> Dict:
    return {
        "id": str(uuid.uuid4()),
        "name": f"Supplier_{random.randint(1000, 9999)}",
        "location": f"Location_{random.randint(1, 5)}",
        "reliability": round(random.uniform(0.7, 1.0), 2),
        "size": random.choice(["Small", "Medium", "Large"]),
        "node_type": "Supplier",
    }


def generate_warehouse() -> Dict:
    return {
        "id": str(uuid.uuid4()),
        "name": f"WH_{random.randint(1000, 9999)}",
        "type": random.choice(["Raw", "WIP", "Finished"]),
        "size": random.choice(["Small", "Medium", "Large"]),
        "location": f"Location_{random.randint(1, 5)}",
        "max_capacity": random.randint(5000, 10000),
        "current_capacity": random.randint(1000, 5000),
        "safety_stock": random.randint(100, 500),
        "node_type": "Warehouse",
    }


def generate_part(level: int) -> Dict:
    return {
        "id": str(uuid.uuid4()),
        "name": f"Part_L{level}_{random.randint(1000, 9999)}",
        "description": f"Manufacturing part level {level}",
        "type": random.choice(["Assembly", "Component", "Raw"]),
        "cost": round(random.uniform(100, 1000), 2),
        "importance": random.randint(1, 10),
        "level": level,
        "units_in_chain": random.randint(100, 1000),
        "node_type": "Parts",
    }


def generate_edge(source_id: str, target_id: str, edge_type: str) -> Dict:
    edge_features = {
        "SupplierToWarehouse": {
            "transportation_cost": round(random.uniform(100, 1000), 2),
            "lead_time": random.randint(1, 14),
        },
        "WarehouseToParts": {
            "inventory_level": random.randint(50, 500),
            "storage_cost": round(random.uniform(10, 100), 2),
        },
        "PartsToFacility": {
            "quantity": random.randint(10, 100),
            "distance_from_warehouse": round(random.uniform(1, 50), 2),
            "transport_cost": round(random.uniform(50, 500), 2),
            "lead_time": random.randint(1, 7),
        },
        "FacilityToProductOfferings": {
            "product_cost": round(random.uniform(500, 5000), 2),
            "lead_time": random.randint(1, 30),
            "quantity_produced": random.randint(100, 1000),
        },
        "WarehouseToProductOfferings": {
            "inventory_level": random.randint(50, 500),
            "storage_cost": round(random.uniform(10, 100), 2),
        },
        "ProductOfferingsToProductFamilies": {},
        "ProductFamiliesToBusinessUnit": {},
    }

    return {
        "source": source_id,
        "target": target_id,
        "key": edge_type,
        **edge_features.get(edge_type, {}),
    }


def send_change(change_data):
    response = requests.post(
        "http://localhost:8000/schema/live/update", json=change_data
    )
    print(f"Sent change: {change_data['action']}")
    print(f"Response: {response.json()}")
    time.sleep(1)  # Wait between changes


def send_schema_action(
    node_id: str,
    node_type: str,
    action_type: str,
    units: int = None,
    properties: Dict = None,
):
    timestamp = int(time.time())

    if action_type == "add_units":
        change_data = {
            "timestamp": timestamp,
            "type": "schema",
            "action": "Update",
            "data": {
                "nodes": {
                    node_type: {
                        node_id: {"units_in_chain": units, "node_type": node_type}
                    }
                },
                "links": [],
            },
        }
    elif action_type == "update_properties":
        change_data = {
            "timestamp": timestamp,
            "type": "schema",
            "action": "Update",
            "data": {
                "nodes": {node_type: {node_id: {**properties, "node_type": node_type}}},
                "links": [],
            },
        }

    send_change(change_data)


def create_and_send_manufacturing_schema():
    nodes = {
        "BusinessUnit": {},
        "ProductFamily": {},
        "ProductOffering": {},
        "Facility": {},
        "Parts": {},
        "Supplier": {},
        "Warehouse": {},
    }
    links = []

    # Create nodes
    business_units = [generate_business_unit() for _ in range(2)]
    product_families = [generate_product_family() for _ in range(4)]
    product_offerings = [generate_product_offering() for _ in range(8)]
    facilities = [generate_facility() for _ in range(5)]
    suppliers = [generate_supplier() for _ in range(3)]
    warehouses = [generate_warehouse() for _ in range(4)]

    # Create parts with levels
    parts = []
    for level in range(4):
        level_parts = [generate_part(level) for _ in range(random.randint(3, 6))]
        parts.extend(level_parts)

    # Add nodes to their respective categories
    for bu in business_units:
        nodes["BusinessUnit"][bu["id"]] = bu
    for pf in product_families:
        nodes["ProductFamily"][pf["id"]] = pf
    for po in product_offerings:
        nodes["ProductOffering"][po["id"]] = po
    for facility in facilities:
        nodes["Facility"][facility["id"]] = facility
    for supplier in suppliers:
        nodes["Supplier"][supplier["id"]] = supplier
    for warehouse in warehouses:
        nodes["Warehouse"][warehouse["id"]] = warehouse
    for part in parts:
        nodes["Parts"][part["id"]] = part

    # Create relationships
    # Supplier to Warehouse
    for supplier in suppliers:
        for warehouse in random.sample(
            warehouses, k=random.randint(1, len(warehouses))
        ):
            links.append(
                generate_edge(supplier["id"], warehouse["id"], "SupplierToWarehouse")
            )

    # Warehouse to Parts
    for warehouse in warehouses:
        for part in random.sample(parts, k=random.randint(2, len(parts))):
            links.append(generate_edge(warehouse["id"], part["id"], "WarehouseToParts"))

    # Parts to Facility
    for part in parts:
        for facility in random.sample(facilities, k=random.randint(1, len(facilities))):
            links.append(generate_edge(part["id"], facility["id"], "PartsToFacility"))

    # Facility to ProductOfferings
    for facility in facilities:
        for offering in random.sample(
            product_offerings, k=random.randint(1, len(product_offerings))
        ):
            links.append(
                generate_edge(
                    facility["id"], offering["id"], "FacilityToProductOfferings"
                )
            )

    # Warehouse to ProductOfferings
    for warehouse in warehouses:
        for offering in random.sample(
            product_offerings, k=random.randint(1, len(product_offerings))
        ):
            links.append(
                generate_edge(
                    warehouse["id"], offering["id"], "WarehouseToProductOfferings"
                )
            )

    # ProductOfferings to ProductFamilies
    for offering in product_offerings:
        family = random.choice(product_families)
        links.append(
            generate_edge(
                offering["id"], family["id"], "ProductOfferingsToProductFamilies"
            )
        )

    # ProductFamilies to BusinessUnit
    for family in product_families:
        unit = random.choice(business_units)
        links.append(
            generate_edge(family["id"], unit["id"], "ProductFamiliesToBusinessUnit")
        )

    # Part composition (for assembly relationships between parts)
    for level in range(1, 4):  # Levels 1-3
        higher_parts = [p for p in parts if p["level"] == level]
        lower_parts = [p for p in parts if p["level"] == level - 1]

        for higher_part in higher_parts:
            # Each higher level part requires 2-3 lower level parts
            required_parts = random.sample(
                lower_parts, k=random.randint(2, min(3, len(lower_parts)))
            )
            for lower_part in required_parts:
                links.append(
                    {
                        "source": higher_part["id"],
                        "target": lower_part["id"],
                        "key": "PartComposition",
                        "quantity_required": random.randint(1, 3),
                    }
                )

    # Send initial schema with all nodes and links
    change = {
        "timestamp": int(time.time()),
        "type": "schema",
        "action": "Create",
        "data": {"nodes": nodes, "links": links},
    }

    send_change(change)


if __name__ == "__main__":
    create_and_send_manufacturing_schema()
