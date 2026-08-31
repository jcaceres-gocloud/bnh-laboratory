import json

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors.file_system import FileSink

def normalizar_y_validar(raw: str) -> str:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return json.dumps(
            {
                "estado_validacion": "INVALIDO",
                "errores": ["json_invalido"],
                "original": raw,
            },
            ensure_ascii=False,
        )

    errores = []

    if "metadata" in data and "registro" in data:
        # Formato envelope, actualmente usado por gRPC.
        metadata = data.get("metadata") or {}
        registro = data.get("registro") or {}

        normalizado = {
            "metadata": {
                "jurisdiccion": metadata.get("jurisdiccion"),
                "dominio": metadata.get("dominio") or "persona",
                "lote_id": metadata.get("lote_id"),
            },
            "registro": registro,
        }

    else:
        # Formato plano, actualmente usado por NiFi.
        normalizado = {
            "metadata": {
                "jurisdiccion": data.get("jurisdiccion"),
                "dominio": "persona",
                "lote_id": None,
            },
            "registro": data,
        }

    id_persona = normalizado["registro"].get("id_persona")

    if isinstance(id_persona, int) and not isinstance(id_persona, bool):
        normalizado["registro"]["id_persona"] = str(id_persona)

    nro_documento = normalizado["registro"].get("nro_documento")

    if isinstance(nro_documento, int) and not isinstance(nro_documento, bool):
        normalizado["registro"]["nro_documento"] = str(nro_documento)

    if not normalizado["metadata"]["jurisdiccion"]:
        errores.append("metadata.jurisdiccion requerida")

    if not normalizado["registro"].get("id_persona"):
        errores.append("registro.id_persona requerido")

    resultado = {
        "estado_validacion": "INVALIDO" if errores else "VALIDO",
        "errores": errores,
        **normalizado,
    }

    return json.dumps(resultado, ensure_ascii=False)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env.enable_checkpointing(5000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:19092")
        .set_topics("bnh.personas")
        .set_group_id("bnh-flink-personas-validation")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    personas = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka - bnh.personas",
    )

    validadas = personas.map(
        normalizar_y_validar,
        output_type=Types.STRING(),
    )

    bronze_sink = (
        FileSink
        .for_row_format(
            "s3://bnh-bronze/personas/",
            Encoder.simple_string_encoder(),
        )
        .build()
    )

    validadas.sink_to(bronze_sink)
    validadas.print()

    env.execute("BNH - Personas Normalize Validate and Bronze")


if __name__ == "__main__":
    main()
