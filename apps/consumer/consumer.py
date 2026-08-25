import json

from confluent_kafka import Consumer


consumer = Consumer(
    {
        "bootstrap.servers": "kafka:19092",
        "group.id": "bnh-personas-consumer",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe(["bnh.personas"])

print("Esperando eventos de bnh.personas...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"ERROR: {msg.error()}")
            continue

        persona = json.loads(msg.value().decode("utf-8"))

        print(
            f"key={msg.key().decode()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"persona={persona}"
        )

finally:
    consumer.close()