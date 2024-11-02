import json
import random
import requests
import time
import uuid
from typing import Dict, List
import argparse


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


def generate_part(level: int, parent_id: str = None, sibling_count: int = 0) -> Dict:
    part_id = generate_hierarchical_id(parent_id, sibling_count)
    return {
        "id": part_id,
        "name": f"Part_L{level}_{random.randint(1000, 9999)}",
        "description": f"Manufacturing part level {level}",
        "type": random.choice(["Assembly", "Component", "Raw"]),
        "cost": round(random.uniform(100, 1000), 2),
        "importance": random.randint(1, 10),
        "level": level,
        "units_in_chain": random.randint(100, 1000),
        "expected_life": random.randint(365, 365 * 5),
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


def send_change(change_data, version: str):
    if "version" not in change_data:
        change_data["version"] = version

    response = requests.post(
        f"http://localhost:8000/schema/live/{version}/update", json=change_data
    )
    print(f"Sent change: {change_data['action']} for version: {version}")
    print(f"Response: {response.json()}")
    time.sleep(1)  # Wait between changes


def send_schema_action(
    node_id: str,
    node_type: str,
    action_type: str,
    version: str,
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

    send_change(change_data, version)


def generate_hierarchical_id(parent_id: str = None, sibling_count: int = 0) -> str:
    if not parent_id:
        return str(sibling_count + 1)
    return f"{parent_id}-{sibling_count + 1}"


def get_parent_id(hierarchical_id: str) -> str:
    parts = hierarchical_id.split("-")
    if len(parts) <= 1:
        return None
    return "-".join(parts[:-1])


def get_child_count(max_child_id: str) -> int:
    if not max_child_id:
        return 0
    return int(max_child_id.split("-")[-1])


def create_and_send_manufacturing_schema(version: str):
    if not version:
        raise ValueError("Version must be specified")

    nodes = {
        "BusinessUnit": {},
        "ProductFamily": {},
        "ProductOffering": {},
        "Facility": {},
        "Supplier": {},
        "Warehouse": {},
        "Parts": {},
    }
    links = []

    # Generate business units (level 1) - reduced count
    business_units = []
    for i in range(2):  # Reduced from random(2,4)
        bu_id = generate_hierarchical_id(sibling_count=i)
        bu = {
            "id": bu_id,
            "name": f"BU_{random.randint(1000, 9999)}",
            "description": f"Business Unit {i+1}",
            "revenue": round(random.uniform(1000000, 5000000), 2),
            "node_type": "BusinessUnit",
        }
        business_units.append(bu)

    # Generate product families (level 2) - reduced count
    product_families = []
    for bu in business_units:
        for j in range(2):  # Reduced from random(2,4)
            pf_id = generate_hierarchical_id(bu["id"], j)
            pf = {
                "id": pf_id,
                "name": f"PF_{random.randint(1000, 9999)}",
                "revenue": round(random.uniform(100000, 1000000), 2),
                "node_type": "ProductFamily",
            }
            product_families.append(pf)

    # Generate product offerings (level 3) - reduced count
    product_offerings = []
    for pf in product_families:
        for k in range(2):  # Reduced from random(2,4)
            po_id = generate_hierarchical_id(pf["id"], k)
            po = {
                "id": po_id,
                "name": f"PO_{random.randint(1000, 9999)}",
                "cost": round(random.uniform(1000, 10000), 2),
                "demand": random.randint(100, 1000),
                "node_type": "ProductOffering",
            }
            product_offerings.append(po)

    # Generate facilities (level 4) - reduced count
    facilities = []
    for po in product_offerings:
        facility_id = generate_hierarchical_id(
            po["id"], 0
        )  # Only 1 facility per offering
        facility = {
            "id": facility_id,
            "name": f"Facility_{random.randint(1000, 9999)}",
            "type": random.choice(["Manufacturing", "Assembly", "Distribution"]),
            "location": f"Location_{random.randint(1, 5)}",
            "max_capacity": random.randint(5000, 10000),
            "operating_cost": round(random.uniform(10000, 50000), 2),
            "node_type": "Facility",
        }
        facilities.append(facility)

    # Generate suppliers (level 5) - reduced count
    suppliers = []
    for facility in facilities:
        supplier_id = generate_hierarchical_id(
            facility["id"], 0
        )  # Only 1 supplier per facility
        supplier = {
            "id": supplier_id,
            "name": f"Supplier_{random.randint(1000, 9999)}",
            "location": f"Location_{random.randint(1, 5)}",
            "reliability": round(random.uniform(0.7, 0.9), 2),
            "size": random.choice(["Small", "Medium", "Large"]),
            "node_type": "Supplier",
        }
        suppliers.append(supplier)

    # Generate warehouses (level 6) - reduced count
    warehouses = []
    for supplier in suppliers:
        warehouse_id = generate_hierarchical_id(
            supplier["id"], 0
        )  # Only 1 warehouse per supplier
        warehouse = {
            "id": warehouse_id,
            "name": f"WH_{random.randint(1000, 9999)}",
            "type": random.choice(["Raw", "WIP", "Finished"]),
            "size": random.choice(["Small", "Medium", "Large"]),
            "location": f"Location_{random.randint(1, 5)}",
            "max_capacity": random.randint(5000, 10000),
            "current_capacity": random.randint(1000, 5000),
            "safety_stock": random.randint(100, 500),
            "node_type": "Warehouse",
        }
        warehouses.append(warehouse)

    # Generate parts hierarchically - reduced count
    parts = []
    for level in range(2, -1, -1):  # Reduced levels from 3 to 2
        if level == 2:
            # Raw materials - attach to suppliers
            for supplier in suppliers:
                for i in range(2):  # Reduced from random(2,4)
                    part = generate_part(level, supplier["id"], i)
                    parts.append(part)
        else:
            # Higher level parts
            for facility in facilities:
                for i in range(2):  # Reduced from random(2,4)
                    part = generate_part(level, facility["id"], i)
                    parts.append(part)

    # Add all nodes to their categories
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

    # Create relationships (reusing existing functions)
    # Reference to existing generate_edge function
    for supplier in suppliers:
        for warehouse in warehouses:
            links.append(
                generate_edge(supplier["id"], warehouse["id"], "SupplierToWarehouse")
            )

    # Other relationships remain the same but with reduced counts
    for warehouse in warehouses:
        for part in random.sample(parts, k=min(2, len(parts))):
            links.append(generate_edge(warehouse["id"], part["id"], "WarehouseToParts"))

    # Send initial schema with explicit version
    change = {
        "timestamp": int(time.time()),
        "type": "schema",
        "action": "Create",
        "data": {"nodes": nodes, "links": links},
        "version": version,
    }

    send_change(change, version)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--versions", nargs="+", default=["v1.0", "v2.0", "v3.0"])
    args = parser.parse_args()

    for version in args.versions:
        print(f"\nGenerating schema for version: {version}")
        try:
            create_and_send_manufacturing_schema(version)
            print(f"Successfully generated schema for version: {version}")
        except Exception as e:
            print(f"Error generating schema for version {version}: {str(e)}")
        time.sleep(2)  # Wait between versions
