import os
import time

import grpc

from personas.v1 import personas_pb2
from personas.v1 import personas_pb2_grpc


TOTAL_PERSONAS = int(os.getenv("TOTAL_PERSONAS", "10"))
GRPC_CASE = os.getenv("GRPC_CASE", "valid")

# Alineado a scripts/testdata/personas.py (persona_base).
# No se importa ese módulo: el cliente Docker no monta scripts/.
REGISTRO_BASE = {
    "id_persona": "P000001",
    "fecha_nacimiento": "1990-05-10",
    "cuit": "20123456789",
    "c_documento": "DNI",
    "nro_documento": "12345678",
    "c_pais_nacimiento": "ARG",
    "c_provincia_nacimiento": "B",
    "c_departamento_nacimiento": "001",
    "c_localidad_nacimiento": "001",
    "c_municipio_nacimiento": "001",
    "lugar_nacimiento": "Buenos Aires",
    "c_fallecido": "N",
    "c_es_indigena": "N",
}

CASOS = {
    "valid": {
        "lote_id": "GRPC-VALID-001",
        "cuit": "20123456789",
    },
    "invalid-cuit": {
        "lote_id": "GRPC-INVALID-CUIT-001",
        "cuit": "2012345678A",
    },
}


def construir_registro(indice, cuit):
    registro = dict(REGISTRO_BASE)
    registro["id_persona"] = f"P{indice:06d}"
    registro["cuit"] = cuit
    return registro


def generar_personas():
    caso = CASOS.get(GRPC_CASE)

    if caso is None:
        conocidos = ", ".join(CASOS)
        raise ValueError(f"GRPC_CASE desconocido: {GRPC_CASE}. Use: {conocidos}")

    lote_id = os.getenv("LOTE_ID") or caso["lote_id"]

    metadata = personas_pb2.Metadata(
        jurisdiccion="ARG-B",
        dominio="persona",
        lote_id=lote_id,
    )

    for i in range(1, TOTAL_PERSONAS + 1):
        yield personas_pb2.CargaPersonaRequest(
            metadata=metadata,
            registro=personas_pb2.Persona(
                **construir_registro(i, caso["cuit"])
            ),
        )


def main():
    channel = grpc.insecure_channel("grpc-server:50051")
    stub = personas_pb2_grpc.PersonasServiceStub(channel)

    inicio = time.perf_counter()

    resultado = stub.EnviarPersonas(generar_personas())

    duracion = time.perf_counter() - inicio

    registros_por_segundo = (
        resultado.cantidad_recibida / duracion if duracion > 0 else 0
    )

    print(f"cantidad_enviada={TOTAL_PERSONAS}")
    print(f"cantidad_recibida={resultado.cantidad_recibida}")
    print(f"lote_id={resultado.lote_id}")
    print(f"estado={resultado.estado}")
    print(f"duracion_segundos={duracion:.3f}")
    print(f"registros_por_segundo={registros_por_segundo:.2f}")


if __name__ == "__main__":
    main()
