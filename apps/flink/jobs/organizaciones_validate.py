import json
from datetime import datetime


CAMPOS_ORGANIZACION = (
    "id_organizacion",
    "nombre",
    "descripcion",
    "c_organizacion",
    "fecha_alta",
    "fecha_baja",
)

CAMPOS_CODIGO = (
    "id_organizacion",
    "c_organizacion",
)

CAMPOS_REQUERIDOS = (
    "id_organizacion",
    "nombre",
    "c_organizacion",
    "fecha_alta",
)

CAMPOS_METADATA_TEXTO = (
    "jurisdiccion",
    "dominio",
    "lote_id",
)


def campo_faltante(valor):
    return valor is None or valor == ""


def exigir_texto(valor, nombre, errores):
    if valor is not None and not isinstance(valor, str):
        errores.append(f"{nombre} debe ser texto")


def resultado_invalido(errores, original):
    return json.dumps(
        {
            "estado_validacion": "INVALIDO",
            "errores": errores,
            "original": original,
        },
        ensure_ascii=False,
    )


def parsear_payload(raw):
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None, ["json_invalido"]

    if not isinstance(data, dict):
        return data, ["payload debe ser objeto"]

    return data, []


def normalizar_estructura(data):
    if "metadata" in data or "registro" in data:
        errores = []

        metadata = data.get("metadata")
        registro = data.get("registro")

        if not isinstance(metadata, dict):
            errores.append("metadata debe ser objeto")

        if not isinstance(registro, dict):
            errores.append("registro debe ser objeto")

        if errores:
            return None, errores

        dominio = metadata.get("dominio")

        return {
            "metadata": {
                "jurisdiccion": metadata.get("jurisdiccion"),
                "dominio": "organizacion" if dominio is None else dominio,
                "lote_id": metadata.get("lote_id"),
            },
            "registro": dict(registro),
        }, []

    return None, ["se requiere envelope {metadata, registro}"]


def completar_campos_organizacion(registro):
    extras = {
        campo: valor
        for campo, valor in registro.items()
        if campo not in CAMPOS_ORGANIZACION
    }

    completado = {
        campo: registro[campo] if campo in registro else None
        for campo in CAMPOS_ORGANIZACION
    }
    completado.update(extras)

    registro.clear()
    registro.update(completado)


def normalizar_campos(registro):
    for campo in CAMPOS_CODIGO:
        valor = registro.get(campo)

        # type(...) is int excluye bool explícitamente.
        if type(valor) is int:
            registro[campo] = str(valor)


def validar_metadata(metadata, errores):
    for campo in CAMPOS_METADATA_TEXTO:
        exigir_texto(metadata.get(campo), f"metadata.{campo}", errores)

    if campo_faltante(metadata.get("jurisdiccion")):
        errores.append("metadata.jurisdiccion requerida")


def validar_campos_requeridos(registro, errores):
    for campo in CAMPOS_REQUERIDOS:
        if campo_faltante(registro.get(campo)):
            errores.append(f"registro.{campo} requerido")


def validar_fecha(registro, campo, errores):
    valor = registro.get(campo)

    if campo_faltante(valor):
        return

    if not isinstance(valor, str):
        errores.append(f"registro.{campo} debe ser texto")
        return

    try:
        fecha = datetime.strptime(valor, "%Y-%m-%d")
    except ValueError:
        errores.append(f"registro.{campo} invalida")
        return

    if fecha.strftime("%Y-%m-%d") != valor:
        errores.append(f"registro.{campo} invalida")


def validar_organizacion(registro, errores):
    validar_campos_requeridos(registro, errores)

    for campo in CAMPOS_CODIGO:
        exigir_texto(registro.get(campo), f"registro.{campo}", errores)

    exigir_texto(registro.get("nombre"), "registro.nombre", errores)
    exigir_texto(registro.get("descripcion"), "registro.descripcion", errores)

    validar_fecha(registro, "fecha_alta", errores)
    validar_fecha(registro, "fecha_baja", errores)


def normalizar_y_validar(raw: str) -> str:
    data, errores = parsear_payload(raw)

    if errores:
        original = raw if data is None else data
        return resultado_invalido(errores, original)

    normalizado, errores = normalizar_estructura(data)

    if errores:
        return resultado_invalido(errores, data)

    registro = normalizado["registro"]
    metadata = normalizado["metadata"]

    completar_campos_organizacion(registro)
    normalizar_campos(registro)

    validar_metadata(metadata, errores)
    validar_organizacion(registro, errores)

    resultado = {
        "estado_validacion": "INVALIDO" if errores else "VALIDO",
        "errores": errores,
        **normalizado,
    }

    return json.dumps(resultado, ensure_ascii=False)


def main():
    # Estos imports quedan acá intencionalmente.
    # Así podemos probar la lógica de validación con Python normal
    # sin necesitar PyFlink instalado en el host.
    from pyflink.common import Types
    from pyflink.common.serialization import Encoder, SimpleStringSchema
    from pyflink.common.watermark_strategy import WatermarkStrategy
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.file_system import FileSink
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaSource,
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(5000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:19092")
        .set_topics("bnh.organizaciones")
        .set_group_id("bnh-flink-organizaciones-validation")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    organizaciones = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka - bnh.organizaciones",
    )

    validadas = organizaciones.map(
        normalizar_y_validar,
        output_type=Types.STRING(),
    )

    bronze_sink = FileSink.for_row_format(
        "s3://bnh-bronze/organizaciones/",
        Encoder.simple_string_encoder(),
    ).build()

    validadas.sink_to(bronze_sink)
    validadas.print()

    env.execute("BNH - Organizaciones Normalize Validate and Bronze")


if __name__ == "__main__":
    main()
