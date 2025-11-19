import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def create_message():
    """Genera un mensaje basado en el dataset real."""

    coffee_types = ["Latte", "Espresso", "Cappuccino", "Mocha", "Americano"]
    cash_types = ["Cash", "Card"]

    now = datetime.now()

    message = {
        "hour_of_day": now.hour,
        "cash_type": random.choice(cash_types),
        "money": round(random.uniform(2.5, 8.5), 2),
        "coffee_name": random.choice(coffee_types),
        "Time_of_Day": "Morning" if now.hour < 12 else "Afternoon",
        "Weekday": now.strftime("%A"),
        "Month_name": now.strftime("%B"),
        "Date": now.strftime("%m/%d/%Y"),
        "Time": now.strftime("%H:%M")
    }

    return message


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("=== Kafka Producer Simulator Iniciado ===")

    while True:
        msg = create_message()
        producer.send("coffee_sales_stream", msg)
        print(f"Mensaje enviado ➜ {msg}")
        time.sleep(1)


if __name__ == "__main__":
    main()