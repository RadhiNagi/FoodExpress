import json
import os
from data import (
    generate_order,
    RESTAURANT_MAPPING,
    CUISINE_MAPPING,
    PAYMENT_METHOD_MAPPING,
    ORDER_STATUS_MAPPING,
    DELIVERY_ZONE_MAPPING,
    CANCELLATION_REASON_MAPPING,
)

def save_json(data, filename):
    filepath = os.path.join("Data", filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved {filepath} ({len(data)} records)")

def save_mapping_tables():
    save_json(RESTAURANT_MAPPING, "map_restaurants.json")
    save_json(CUISINE_MAPPING, "map_cuisines.json")
    save_json(PAYMENT_METHOD_MAPPING, "map_payment_methods.json")
    save_json(ORDER_STATUS_MAPPING, "map_order_statuses.json")
    save_json(DELIVERY_ZONE_MAPPING, "map_delivery_zones.json")
    save_json(CANCELLATION_REASON_MAPPING, "map_cancellation_reasons.json")

def generate_bulk_orders(count=1000):
    orders = []
    for i in range(count):
        order = generate_order()
        orders.append(order)
    
    filepath = os.path.join("Data", "bulk_orders.json")
    with open(filepath, "w") as f:
        json.dump(orders, f, indent=2)
    
    print(f"Saved {filepath} ({len(orders)} orders)")
    print(f"Sample order_id: {orders[0]['order_id']}")
    print(f"Sample restaurant_id: {orders[0]['restaurant_id']}")
    print(f"Sample total_amount: ${orders[0]['total_amount']}")


def save_files_array():
    files_array = [
        {"file": "map_restaurants"},
        {"file": "map_cuisines"},
        {"file": "map_payment_methods"},
        {"file": "map_order_statuses"},
        {"file": "map_delivery_zones"},
        {"file": "map_cancellation_reasons"},
    ]
    with open("files_array.json", "w") as f:
        json.dump(files_array, f, indent=2)
    print(f"Saved files_array.json ({len(files_array)} files)")

if __name__ == "__main__":
    print("=" * 50)
    print("FOODEXPRESS DATA GENERATOR")
    print("=" * 50)
    
    print("\n--- Saving mapping tables ---")
    save_mapping_tables()
    
    print("\n--- Generating bulk orders ---")
    generate_bulk_orders(1000)
    
    print("\n--- Saving ADF config ---")
    save_files_array()
    
    print("\n" + "=" * 50)
    print("All files generated successfully!")
    print("=" * 50)