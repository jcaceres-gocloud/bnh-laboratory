import json

from confluent_kafka import Producer


producer = Producer(
    {
        "bootstrap.servers": "kafka:19092",
    }
)

personas = [
    {"id": "P101", "nombre": "Carlos", "jurisdiccion": "ARG-B"},
    {"id": "P102", "nombre": "Laura", "jurisdiccion": "ARG-B"},
    {"id": "P103", "nombre": "Martin", "jurisdiccion": "ARG-B"},
]


def delivery_report(err, msg):
    if err:
        print(f"ERROR: {err}")
        return

    print(
        f"OK key={msg.key().decode()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


for persona in personas:
    producer.produce(
        topic="bnh.personas",
        key=persona["id"],
        value=json.dumps(persona),
        callback=delivery_report,
    )

producer.flush()