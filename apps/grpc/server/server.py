import json
from concurrent import futures
import time

import grpc
from confluent_kafka import Producer

from personas.v1 import personas_pb2
from personas.v1 import personas_pb2_grpc

from prometheus_client import Counter, Gauge, Histogram, start_http_server


producer = Producer(
    {
        "bootstrap.servers": "kafka:19092",
    }
)

PERSONAS_RECEIVED = Counter(
    "bnh_grpc_personas_received_total",
    "Cantidad total de personas recibidas por gRPC",
)

KAFKA_ENQUEUED = Counter(
    "bnh_grpc_kafka_enqueued_total",
    "Cantidad total de personas encoladas en el producer de Kafka",
)

BATCHES_TOTAL = Counter(
    "bnh_grpc_batches_total",
    "Cantidad total de lotes procesados por gRPC",
)

BATCH_DURATION = Histogram(
    "bnh_grpc_batch_duration_seconds",
    "Duracion del procesamiento de lotes gRPC en segundos",
)

LAST_BATCH_RECORDS = Gauge(
    "bnh_grpc_last_batch_records",
    "Cantidad de registros procesados en el ultimo lote gRPC",
)

LAST_BATCH_DURATION = Gauge(
    "bnh_grpc_last_batch_duration_seconds",
    "Duracion del ultimo lote gRPC en segundos",
)

LAST_BATCH_THROUGHPUT = Gauge(
    "bnh_grpc_last_batch_throughput",
    "Throughput del ultimo lote gRPC en registros por segundo",
)

class PersonasService(personas_pb2_grpc.PersonasServiceServicer):
    def EnviarPersonas(self, request_iterator, context):
        inicio = time.perf_counter()

        cantidad = 0
        lote_id = ""

        for request in request_iterator:
            cantidad += 1
            lote_id = request.metadata.lote_id

            PERSONAS_RECEIVED.inc()

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

            KAFKA_ENQUEUED.inc()

            producer.poll(0)

        pendientes = producer.flush(10)

        if pendientes > 0:
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"No se pudieron publicar {pendientes} mensajes en Kafka",
            )

        duracion = time.perf_counter() - inicio

        LAST_BATCH_RECORDS.set(cantidad)
        LAST_BATCH_DURATION.set(duracion)

        if duracion > 0:
            LAST_BATCH_THROUGHPUT.set(cantidad / duracion)
        else:
            LAST_BATCH_THROUGHPUT.set(0)

        BATCHES_TOTAL.inc()
        BATCH_DURATION.observe(duracion)

        return personas_pb2.ResultadoCarga(
            lote_id=lote_id,
            cantidad_recibida=cantidad,
            estado="RECIBIDO",
        )

def serve():
    start_http_server(8001)
    print("Métricas Prometheus disponibles en puerto 8001")

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
