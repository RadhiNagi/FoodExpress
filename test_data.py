import unittest
from data import (
    generate_order,
    RESTAURANT_MAPPING,
    CUISINE_MAPPING,
    PAYMENT_METHOD_MAPPING,
    ORDER_STATUS_MAPPING,
    DELIVERY_ZONE_MAPPING,
    CANCELLATION_REASON_MAPPING,
    RESTAURANT_LIST,
    ZONE_LIST,
)


class TestMappingTables(unittest.TestCase):
    """Test that all mapping tables have correct structure and data."""

    def test_restaurant_count(self):
        self.assertEqual(len(RESTAURANT_MAPPING), 10)

    def test_cuisine_count(self):
        self.assertEqual(len(CUISINE_MAPPING), 6)

    def test_payment_method_count(self):
        self.assertEqual(len(PAYMENT_METHOD_MAPPING), 4)

    def test_order_status_count(self):
        self.assertEqual(len(ORDER_STATUS_MAPPING), 3)

    def test_delivery_zone_count(self):
        self.assertEqual(len(DELIVERY_ZONE_MAPPING), 10)

    def test_cancellation_reason_count(self):
        self.assertEqual(len(CANCELLATION_REASON_MAPPING), 4)

    def test_restaurant_has_required_fields(self):
        for r in RESTAURANT_MAPPING:
            self.assertIn("restaurant_id", r)
            self.assertIn("restaurant_name", r)
            self.assertIn("cuisine_id", r)
            self.assertIn("avg_prep_time_min", r)

    def test_restaurant_ids_unique(self):
        ids = [r["restaurant_id"] for r in RESTAURANT_MAPPING]
        self.assertEqual(len(ids), len(set(ids)))

    def test_zone_ids_unique(self):
        ids = [z["zone_id"] for z in DELIVERY_ZONE_MAPPING]
        self.assertEqual(len(ids), len(set(ids)))

    def test_cuisine_ids_valid_in_restaurants(self):
        valid_cuisine_ids = {c["cuisine_id"] for c in CUISINE_MAPPING}
        for r in RESTAURANT_MAPPING:
            self.assertIn(r["cuisine_id"], valid_cuisine_ids)


class TestOrderGenerator(unittest.TestCase):
    """Test that generate_order produces valid orders."""

    def setUp(self):
        """Generate one order to test — runs before EACH test method."""
        self.order = generate_order()

    def test_order_has_all_required_fields(self):
        required_fields = [
            "order_id", "customer_id", "driver_id",
            "restaurant_id", "cuisine_id", "zone_id",
            "payment_method_id", "order_status_id",
            "cancellation_reason_id",
            "customer_name", "customer_email",
            "total_amount", "food_cost", "delivery_fee",
            "order_timestamp", "pickup_timestamp", "delivery_timestamp",
        ]
        for field in required_fields:
            self.assertIn(field, self.order, f"Missing field: {field}")

    def test_order_id_is_uuid_format(self):
        self.assertEqual(len(self.order["order_id"]), 36)
        self.assertEqual(self.order["order_id"].count("-"), 4)

    def test_restaurant_id_valid(self):
        valid_ids = {r["restaurant_id"] for r in RESTAURANT_MAPPING}
        self.assertIn(self.order["restaurant_id"], valid_ids)

    def test_zone_id_valid(self):
        valid_ids = {z["zone_id"] for z in DELIVERY_ZONE_MAPPING}
        self.assertIn(self.order["zone_id"], valid_ids)

    def test_payment_method_id_valid(self):
        valid_ids = {p["payment_method_id"] for p in PAYMENT_METHOD_MAPPING}
        self.assertIn(self.order["payment_method_id"], valid_ids)

    def test_total_amount_positive(self):
        self.assertGreater(self.order["total_amount"], 0)

    def test_food_cost_positive(self):
        self.assertGreater(self.order["food_cost"], 0)

    def test_delivery_fee_positive(self):
        self.assertGreater(self.order["delivery_fee"], 0)

    def test_driver_rating_in_range(self):
        rating = self.order["driver_rating"]
        self.assertGreaterEqual(rating, 3.5)
        self.assertLessEqual(rating, 5.0)

    def test_item_count_in_range(self):
        count = self.order["item_count"]
        self.assertGreaterEqual(count, 1)
        self.assertLessEqual(count, 6)

    def test_delivery_time_in_range(self):
        minutes = self.order["delivery_time_minutes"]
        self.assertGreaterEqual(minutes, 8)
        self.assertLessEqual(minutes, 40)

    def test_latitude_in_us_range(self):
        lat = self.order["delivery_latitude"]
        self.assertGreaterEqual(lat, 25.0)
        self.assertLessEqual(lat, 48.0)

    def test_longitude_in_us_range(self):
        lng = self.order["delivery_longitude"]
        self.assertGreaterEqual(lng, -122.0)
        self.assertLessEqual(lng, -73.0)

    def test_cancelled_order_has_reason(self):
        if self.order["order_status_id"] == 2:
            self.assertIn(self.order["cancellation_reason_id"], [1, 2, 3])

    def test_completed_order_has_no_reason(self):
        if self.order["order_status_id"] == 1:
            self.assertEqual(self.order["cancellation_reason_id"], 4)


class TestBulkGeneration(unittest.TestCase):
    """Test generating multiple orders at once."""

    def test_hundred_orders_all_valid(self):
        orders = [generate_order() for _ in range(100)]
        self.assertEqual(len(orders), 100)

        for order in orders:
            self.assertIsNotNone(order["order_id"])
            self.assertGreater(order["total_amount"], 0)

    def test_hundred_orders_unique_ids(self):
        orders = [generate_order() for _ in range(100)]
        ids = [o["order_id"] for o in orders]
        self.assertEqual(len(ids), len(set(ids)))

    def test_cancellation_rate_roughly_ten_percent(self):
        orders = [generate_order() for _ in range(1000)]
        cancelled = sum(1 for o in orders if o["order_status_id"] == 2)
        # Allow 5%-18% range (random variation with 1000 samples)
        self.assertGreater(cancelled, 50)
        self.assertLess(cancelled, 180)


if __name__ == "__main__":
    unittest.main()
