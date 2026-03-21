import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

RESTAURANT_MAPPING = [
    {"restaurant_id": 1, "restaurant_name": "Taj Kitchen", "cuisine_id": 1, "avg_prep_time_min": 25},
    {"restaurant_id": 2, "restaurant_name": "Pizza Palace", "cuisine_id": 2, "avg_prep_time_min": 20},
    {"restaurant_id": 3, "restaurant_name": "Dragon Wok", "cuisine_id": 3, "avg_prep_time_min": 18},
    {"restaurant_id": 4, "restaurant_name": "Burrito Bandits", "cuisine_id": 4, "avg_prep_time_min": 15},
    {"restaurant_id": 5, "restaurant_name": "Classic Diner", "cuisine_id": 5, "avg_prep_time_min": 22},
    {"restaurant_id": 6, "restaurant_name": "Sushi Zen", "cuisine_id": 6, "avg_prep_time_min": 30},
    {"restaurant_id": 7, "restaurant_name": "Spice Route", "cuisine_id": 1, "avg_prep_time_min": 28},
    {"restaurant_id": 8, "restaurant_name": "Napoli Express", "cuisine_id": 2, "avg_prep_time_min": 17},
    {"restaurant_id": 9, "restaurant_name": "Golden Dragon", "cuisine_id": 3, "avg_prep_time_min": 20},
    {"restaurant_id": 10, "restaurant_name": "Taco Fiesta", "cuisine_id": 4, "avg_prep_time_min": 12},
]

CUISINE_MAPPING = [
    {"cuisine_id": 1, "cuisine_type": "Indian"},
    {"cuisine_id": 2, "cuisine_type": "Italian"},
    {"cuisine_id": 3, "cuisine_type": "Chinese"},
    {"cuisine_id": 4, "cuisine_type": "Mexican"},
    {"cuisine_id": 5, "cuisine_type": "American"},
    {"cuisine_id": 6, "cuisine_type": "Japanese"},
]

PAYMENT_METHOD_MAPPING = [
    {"payment_method_id": 1, "payment_method": "Credit Card", "is_digital": True},
    {"payment_method_id": 2, "payment_method": "Debit Card", "is_digital": True},
    {"payment_method_id": 3, "payment_method": "Digital Wallet", "is_digital": True},
    {"payment_method_id": 4, "payment_method": "Cash", "is_digital": False},
]

ORDER_STATUS_MAPPING = [
    {"order_status_id": 1, "order_status": "Delivered", "is_completed": True},
    {"order_status_id": 2, "order_status": "Cancelled", "is_completed": False},
    {"order_status_id": 3, "order_status": "Refunded", "is_completed": False},
]

DELIVERY_ZONE_MAPPING = [
    {"zone_id": 1, "zone_name": "Manhattan-Midtown", "city": "New York", "state": "NY", "region": "Northeast"},
    {"zone_id": 2, "zone_name": "Brooklyn-Downtown", "city": "New York", "state": "NY", "region": "Northeast"},
    {"zone_id": 3, "zone_name": "LA-Hollywood", "city": "Los Angeles", "state": "CA", "region": "West"},
    {"zone_id": 4, "zone_name": "Chicago-Loop", "city": "Chicago", "state": "IL", "region": "Midwest"},
    {"zone_id": 5, "zone_name": "Houston-Galleria", "city": "Houston", "state": "TX", "region": "South"},
    {"zone_id": 6, "zone_name": "Phoenix-Downtown", "city": "Phoenix", "state": "AZ", "region": "Southwest"},
    {"zone_id": 7, "zone_name": "Queens-Astoria", "city": "New York", "state": "NY", "region": "Northeast"},
    {"zone_id": 8, "zone_name": "SF-Mission", "city": "San Francisco", "state": "CA", "region": "West"},
    {"zone_id": 9, "zone_name": "Dallas-Uptown", "city": "Dallas", "state": "TX", "region": "South"},
    {"zone_id": 10, "zone_name": "Seattle-Capitol", "city": "Seattle", "state": "WA", "region": "West"},
]

CANCELLATION_REASON_MAPPING = [
    {"cancellation_reason_id": 1, "cancellation_reason": "Customer cancelled"},
    {"cancellation_reason_id": 2, "cancellation_reason": "Restaurant closed"},
    {"cancellation_reason_id": 3, "cancellation_reason": "Driver unavailable"},
    {"cancellation_reason_id": 4, "cancellation_reason": None},
]


RESTAURANT_LIST = [r["restaurant_name"] for r in RESTAURANT_MAPPING]
RESTAURANT_ID_MAP = {r["restaurant_name"]: r["restaurant_id"] for r in RESTAURANT_MAPPING}

PAYMENT_METHODS_LIST = [p["payment_method"] for p in PAYMENT_METHOD_MAPPING]
PAYMENT_METHOD_ID_MAP = {p["payment_method"]: p["payment_method_id"] for p in PAYMENT_METHOD_MAPPING}

ZONE_LIST = [z["zone_name"] for z in DELIVERY_ZONE_MAPPING]
ZONE_ID_MAP = {z["zone_name"]: z["zone_id"] for z in DELIVERY_ZONE_MAPPING}

def generate_order():

    # Timestamps — the order journey
    order_time = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
    prep_time_min = random.randint(10, 45)
    delivery_time_min = random.randint(8, 40)
    picked_up_time = order_time + timedelta(minutes=prep_time_min)
    delivered_time = picked_up_time + timedelta(minutes=delivery_time_min)

    # Restaurant and zone selection
    restaurant = random.choice(RESTAURANT_MAPPING)
    restaurant_id = restaurant["restaurant_id"]
    restaurant_name = restaurant["restaurant_name"]
    cuisine_id = restaurant["cuisine_id"]

    delivery_zone = random.choice(DELIVERY_ZONE_MAPPING)
    zone_id = delivery_zone["zone_id"]

    # Pricing — how food delivery charges work
    item_count = random.randint(1, 6)
    food_cost = round(random.uniform(8.0, 75.0), 2)
    delivery_fee = round(random.uniform(2.0, 8.0), 2)
    tax_amount = round(food_cost * 0.08, 2)
    discount = round(random.choice([0, 0, 0, 2, 5, 10, food_cost * 0.1]), 2)
    tip = round(random.choice([0, 0, 2, 3, 5, round(random.uniform(1, 15), 2)]), 2)
    total_amount = round(food_cost + delivery_fee + tax_amount - discount + tip, 2)

    # Order status — 10% chance of cancellation
    is_cancelled = random.random() < 0.10
    if is_cancelled:
        order_status_id = 2
        cancellation_reason = random.choice(["Customer cancelled", "Restaurant closed", "Driver unavailable"])
        cancellation_reason_id = {"Customer cancelled": 1, "Restaurant closed": 2, "Driver unavailable": 3}[cancellation_reason]
    else:
        order_status_id = 1
        cancellation_reason = None
        cancellation_reason_id = 4

    # Payment method
    payment = random.choice(PAYMENT_METHOD_MAPPING)
    payment_method_id = payment["payment_method_id"]


    # Build the complete order
    order = {
        # Unique IDs
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "driver_id": str(uuid.uuid4()),

        # Foreign keys to mapping tables
        "restaurant_id": restaurant_id,
        "cuisine_id": cuisine_id,
        "zone_id": zone_id,
        "payment_method_id": payment_method_id,
        "order_status_id": order_status_id,
        "cancellation_reason_id": cancellation_reason_id,

        # Customer info
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "customer_phone": fake.phone_number(),
        "delivery_address": fake.address().replace("\n", ", "),

        # Driver info
        "driver_name": fake.name(),
        "driver_rating": round(random.uniform(3.5, 5.0), 2),
        "driver_phone": fake.phone_number(),

        # Location
        "delivery_latitude": round(random.uniform(25.0, 48.0), 6),
        "delivery_longitude": round(random.uniform(-122.0, -73.0), 6),

        # Timestamps
        "order_timestamp": order_time.isoformat(),
        "pickup_timestamp": picked_up_time.isoformat(),
        "delivery_timestamp": delivered_time.isoformat(),

        # Measures — the numbers analysts will aggregate
        "item_count": item_count,
        "food_cost": food_cost,
        "delivery_fee": delivery_fee,
        "tax_amount": tax_amount,
        "discount": discount,
        "tip_amount": tip,
        "total_amount": total_amount,
        "prep_time_minutes": prep_time_min,
        "delivery_time_minutes": delivery_time_min,

        # Rating
        "rating": random.choice([None, random.randint(1, 5)]),
    }

    return order







