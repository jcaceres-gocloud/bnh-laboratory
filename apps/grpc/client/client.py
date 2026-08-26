import grpc

from personas.v1 import personas_pb2
from personas.v1 import personas_pb2_grpc


def generar_personas():
    metadata = personas_pb2.Metadata(
        jurisdiccion="ARG-B",
        dominio="persona",
        lote_id="L-GRPC-001",
    )

    personas = [
        ("P501", "Lucia"),
        ("P502", "Mateo"),
        ("P503", "Camila"),
    ]

    for persona_id, nombre in personas:
        yield personas_pb2.CargaPersonaRequest(
            metadata=metadata,
            registro=personas_pb2.Persona(
                id=persona_id,
                nombre=nombre,
            ),
        )


def main():
    channel = grpc.insecure_channel("grpc-server:50051")

    stub = personas_pb2_grpc.PersonasServiceStub(channel)

    resultado = stub.EnviarPersonas(generar_personas())

    print(
        f"lote_id={resultado.lote_id} "
        f"cantidad_recibida={resultado.cantidad_recibida} "
        f"estado={resultado.estado}"
    )


if __name__ == "__main__":
    main()
