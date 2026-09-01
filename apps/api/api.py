import json
from typing import Any

from confluent_kafka import Producer
from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, model_validator


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
    model_config = ConfigDict(extra="allow")

    id_persona: Any = None
    fecha_nacimiento: Any = None
    cuit: Any = None
    c_documento: Any = None
    nro_documento: Any = None
    c_pais_nacimiento: Any = None
    c_provincia_nacimiento: Any = None
    c_departamento_nacimiento: Any = None
    c_localidad_nacimiento: Any = None
    c_municipio_nacimiento: Any = None
    lugar_nacimiento: Any = None
    c_fallecido: Any = None
    fecha_fallecido: Any = None
    c_es_indigena: Any = None


class PersonasPayload(BaseModel):
    metadata: Metadata
    registro: Persona | None = None
    registros: list[Persona] | None = None

    @model_validator(mode="after")
    def exigir_registro(self):
        if self.registro is None and self.registros is None:
            raise ValueError("se requiere registro o registros")
        return self


def personas_del_payload(payload: PersonasPayload) -> list[Persona]:
    if payload.registros is not None:
        return payload.registros

    return [payload.registro]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/personas", status_code=202)
def crear_personas(payload: PersonasPayload):
    personas = personas_del_payload(payload)
    metadata = payload.metadata.model_dump()

    for persona in personas:
        registro = persona.model_dump(exclude_unset=True)
        evento = {
            "metadata": metadata,
            "registro": registro,
        }

        id_persona = registro.get("id_persona")
        if id_persona is None:
            id_persona = ""

        producer.produce(
            topic="bnh.personas",
            key=f"{payload.metadata.jurisdiccion}:{id_persona}",
            value=json.dumps(evento, ensure_ascii=False),
        )

    producer.flush()

    return {
        "status": "accepted",
        "lote_id": payload.metadata.lote_id,
        "cantidad_registros": len(personas),
    }
