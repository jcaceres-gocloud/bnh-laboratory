import os
import time

import grpc

from organizaciones.v1 import organizaciones_pb2
from organizaciones.v1 import organizaciones_pb2_grpc


TOTAL_ORGANIZACIONES = int(os.getenv("TOTAL_ORGANIZACIONES", "10"))
GRPC_CASE = os.getenv("GRPC_CASE", "valid")

# Alineado a scripts/testdata/organizaciones.py (organizacion_base).
# No se importa ese módulo: el cliente Docker no monta scripts/.
REGISTRO_BASE = {
    "id_organizacion": "ORG000001",
    "nombre": "Hospital Central",
    "descripcion": "Organizacion de prueba del laboratorio",
    "c_organizacion": "01",
    "fecha_alta": "2020-01-15",
}

CASOS = {
    "valid": {
        "lote_id": "GRPC-ORG-VALID-001",
        "fecha_alta": "2020-01-15",
    },
    "invalid-start-date": {
        "lote_id": "GRPC-ORG-INVALID-001",
        "fecha_alta": "2020-99-99",
    },
}


def construir_registro(indice, fecha_alta):
    registro = dict(REGISTRO_BASE)
    registro["id_organizacion"] = f"ORG{indice:06d}"
    registro["fecha_alta"] = fecha_alta
    return registro


def generar_organizaciones():
    caso = CASOS.get(GRPC_CASE)

    if caso is None:
        conocidos = ", ".join(CASOS)
        raise ValueError(f"GRPC_CASE desconocido: {GRPC_CASE}. Use: {conocidos}")

    lote_id = os.getenv("LOTE_ID") or caso["lote_id"]

    metadata = organizaciones_pb2.Metadata(
        jurisdiccion="ARG-B",
        dominio="organizacion",
        lote_id=lote_id,
    )

    for i in range(1, TOTAL_ORGANIZACIONES + 1):
        yield organizaciones_pb2.CargaOrganizacionRequest(
            metadata=metadata,
            registro=organizaciones_pb2.Organizacion(
                **construir_registro(i, caso["fecha_alta"])
            ),
        )


def main():
    channel = grpc.insecure_channel("grpc-server:50051")
    stub = organizaciones_pb2_grpc.OrganizacionesServiceStub(channel)

    inicio = time.perf_counter()

    resultado = stub.EnviarOrganizaciones(generar_organizaciones())

    duracion = time.perf_counter() - inicio

    registros_por_segundo = (
        resultado.cantidad_recibida / duracion if duracion > 0 else 0
    )

    print(f"cantidad_enviada={TOTAL_ORGANIZACIONES}")
    print(f"cantidad_recibida={resultado.cantidad_recibida}")
    print(f"lote_id={resultado.lote_id}")
    print(f"estado={resultado.estado}")
    print(f"duracion_segundos={duracion:.3f}")
    print(f"registros_por_segundo={registros_por_segundo:.2f}")


if __name__ == "__main__":
    main()
