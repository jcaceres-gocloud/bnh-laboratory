import json

from confluent_kafka import Producer
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(title="BNH Laboratory API")


producer = Producer(
    {
        "bootstrap.servers": "kafka:19092",
    }
)


class Metadata(BaseModel):
    jurisdiccion: str
    dominio: str
    lote_id: str


class Persona(BaseModel):
    id: str
    nombre: str


class PersonasPayload(BaseModel):
    metadata: Metadata
    registros: list[Persona]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/personas", status_code=202)
def crear_personas(payload: PersonasPayload):
    for persona in payload.registros:
        evento = {
            "metadata": payload.metadata.model_dump(),
            "registro": persona.model_dump(),
        }

        key = f"{payload.metadata.jurisdiccion}:{persona.id}"

        producer.produce(
            topic="bnh.personas",
            key=key,
            value=json.dumps(evento),
        )

    producer.flush()

    return {
        "status": "accepted",
        "lote_id": payload.metadata.lote_id,
        "cantidad_registros": len(payload.registros),
    }