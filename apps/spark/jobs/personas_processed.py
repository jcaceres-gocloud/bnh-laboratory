from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BRONZE_PATH = "s3a://bnh-bronze/personas/*/part-*"


spark = (
    SparkSession.builder
    .appName("BNH - Personas Bronze to Processed")
    .getOrCreate()
)

bronze = spark.read.json(BRONZE_PATH)

candidatos = (
    bronze
    .filter(
        (F.col("estado_validacion") == "VALIDO")
        & (F.col("metadata.dominio") == "persona")
        & F.col("registro.id_persona").isNotNull()
    )
    .withColumn(
        "fecha_nacimiento_parsed",
        F.expr("try_cast(registro.fecha_nacimiento AS DATE)")
    )
    .withColumn(
        "fecha_fallecido_parsed",
        F.expr("try_cast(registro.fecha_fallecido AS DATE)")
    )
)

aptos = candidatos.filter(
    F.col("fecha_nacimiento_parsed").isNotNull()
    & (
        F.col("registro.fecha_fallecido").isNull()
        | F.col("fecha_fallecido_parsed").isNotNull()
    )
)

processed = aptos.select(
    F.col("registro.id_persona").alias("id_persona"),
    F.col("metadata.jurisdiccion").alias("jurisdiccion"),
    F.col("metadata.lote_id").alias("lote_id"),
    F.col("fecha_nacimiento_parsed").alias("fecha_nacimiento"),
    F.col("registro.cuit").alias("cuit"),
    F.col("registro.c_documento").alias("c_documento"),
    F.col("registro.nro_documento").alias("nro_documento"),
    F.col("registro.c_pais_nacimiento").alias("c_pais_nacimiento"),
    F.col("registro.c_provincia_nacimiento").alias("c_provincia_nacimiento"),
    F.col("registro.c_departamento_nacimiento").alias("c_departamento_nacimiento"),
    F.col("registro.c_localidad_nacimiento").alias("c_localidad_nacimiento"),
    F.col("registro.c_municipio_nacimiento").alias("c_municipio_nacimiento"),
    F.col("registro.lugar_nacimiento").alias("lugar_nacimiento"),
    F.col("registro.c_fallecido").alias("c_fallecido"),
    F.col("fecha_fallecido_parsed").alias("fecha_fallecido"),
    F.col("registro.c_es_indigena").alias("c_es_indigena"),
    F.current_timestamp().alias("processed_at"),
)

print(f"Candidatos: {candidatos.count()}")
print(f"Descartados por tipado: {candidatos.count() - aptos.count()}")
print(f"Processed: {processed.count()}")

processed.show(20, truncate=False)

JDBC_URL = "jdbc:postgresql://postgres-dw:5432/bnh_dw"

(
    processed.write
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "processed.persona")
    .option("user", "bnh")
    .option("password", "BnhLaboratory1234")
    .option("driver", "org.postgresql.Driver")
    .option("truncate", "true")
    .mode("overwrite")
    .save()
)

print("Carga PostgreSQL completada")

spark.stop()
