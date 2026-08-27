import os
import time

import grpc

from personas.v1 import personas_pb2
from personas.v1 import personas_pb2_grpc


TOTAL_PERSONAS = int(os.getenv("TOTAL_PERSONAS", "10"))


def generar_personas():
    metadata = personas_pb2.Metadata(
        jurisdiccion="ARG-B",
        dominio="persona",
        lote_id=f"L-GRPC-{TOTAL_PERSONAS}",
    )

    for i in range(1, TOTAL_PERSONAS + 1):
        yield personas_pb2.CargaPersonaRequest(
            metadata=metadata,
            registro=personas_pb2.Persona(
                id=f"P{i:06d}",
                nombre=f"Persona {i}",
            ),
        )


def main():
    channel = grpc.insecure_channel("grpc-server:50051")
    stub = personas_pb2_grpc.PersonasServiceStub(channel)

    inicio = time.perf_counter()

    resultado = stub.EnviarPersonas(generar_personas())

    duracion = time.perf_counter() - inicio

    registros_por_segundo = (
        resultado.cantidad_recibida / duracion
        if duracion > 0
        else 0
    )

    print(f"cantidad_enviada={TOTAL_PERSONAS}")
    print(f"cantidad_recibida={resultado.cantidad_recibida}")
    print(f"lote_id={resultado.lote_id}")
    print(f"estado={resultado.estado}")
    print(f"duracion_segundos={duracion:.3f}")
    print(f"registros_por_segundo={registros_por_segundo:.2f}")


if __name__ == "__main__":
    main()