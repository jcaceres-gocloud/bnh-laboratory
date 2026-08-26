import json
from concurrent import futures

import grpc
from confluent_kafka import Producer

from personas.v1 import personas_pb2
from personas.v1 import personas_pb2_grpc


producer = Producer(
    {
        "bootstrap.servers": "kafka:19092",
    }
)


class PersonasService(personas_pb2_grpc.PersonasServiceServicer):
    def EnviarPersonas(self, request_iterator, context):
        cantidad = 0
        lote_id = ""

        for request in request_iterator:
            cantidad += 1
            lote_id = request.metadata.lote_id

            evento = {
                "metadata": {
                    "jurisdiccion": request.metadata.jurisdiccion,
                    "dominio": request.metadata.dominio,
                    "lote_id": request.metadata.lote_id,
                },
                "registro": {
                    "id": request.registro.id,
                    "nombre": request.registro.nombre,
                },
            }

            key = (
                f"{request.metadata.jurisdiccion}:"
                f"{request.registro.id}"
            )

            producer.produce(
                topic="bnh.personas",
                key=key,
                value=json.dumps(evento),
            )

            producer.poll(0)

            print(
                f"Publicada persona "
                f"key={key} "
                f"lote_id={request.metadata.lote_id}"
            )

        pendientes = producer.flush(10)

        if pendientes > 0:
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"No se pudieron publicar {pendientes} mensajes en Kafka",
            )

        return personas_pb2.ResultadoCarga(
            lote_id=lote_id,
            cantidad_recibida=cantidad,
            estado="RECIBIDO",
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    personas_pb2_grpc.add_PersonasServiceServicer_to_server(
        PersonasService(),
        server,
    )

    server.add_insecure_port("[::]:50051")
    server.start()

    print("Servidor gRPC escuchando en puerto 50051")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
