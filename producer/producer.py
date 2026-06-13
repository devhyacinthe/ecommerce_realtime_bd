import json, os, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")
EPS = float(os.getenv("EVENTS_PER_SECOND", "20"))

CATEGORIES = {
    "Electronics": [("Laptop", 950), ("Smartphone", 600), ("Headphones", 120)],
    "Fashion":     [("Sneakers", 90), ("Jacket", 140), ("T-Shirt", 25)],
    "Home":        [("Coffee Maker", 75), ("Lamp", 40), ("Chair", 110)],
    "Books":       [("Novel", 15), ("Cookbook", 30), ("Comic", 12)],
}
COUNTRIES = ["FR", "TG", "US", "DE", "NG", "GB", "CI", "MA"]
PAYMENTS = ["card", "paypal", "mobile_money", "bank_transfer"]
# Entonnoir réaliste : vue >> panier >> achat
EVENT_TYPES = ["view"] * 4 + ["add_to_cart"] * 2 + ["purchase"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
)

def make_event():
    category = random.choice(list(CATEGORIES))
    name, base_price = random.choice(CATEGORIES[category])
    etype = random.choice(EVENT_TYPES)
    evt = {
        "event_id": str(uuid.uuid4()),
        "event_type": etype,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": f"u_{random.randint(1, 5000)}",
        "session_id": f"s_{random.randint(1, 20000)}",
        "product_id": f"p_{abs(hash(name)) % 1000}",
        "product_name": name,
        "category": category,
        "price": round(base_price * random.uniform(0.9, 1.1), 2),
        "quantity": random.randint(1, 3) if etype == "purchase" else 1,
        "country": random.choice(COUNTRIES),
    }
    if etype == "purchase":
        evt["payment_method"] = random.choice(PAYMENTS)
    return evt

if __name__ == "__main__":
    print(f"Producing to {BOOTSTRAP} topic={TOPIC} at ~{EPS} eps", flush=True)
    interval = 1.0 / EPS
    while True:
        producer.send(TOPIC, make_event())
        time.sleep(interval)