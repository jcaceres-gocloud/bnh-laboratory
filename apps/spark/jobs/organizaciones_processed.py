from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BRONZE_PATH = "s3a://bnh-bronze/organizaciones/*/part-*"
JDBC_URL = "jdbc:postgresql://postgres-dw:5432/bnh_dw"


spark = (
    SparkSession.builder
    .appName("BNH - Organizaciones Bronze to Processed")
    .getOrCreate()
)

bronze = spark.read.json(BRONZE_PATH)

candidatos = (
    bronze
    .filter(
        (F.col("estado_validacion") == "VALIDO")
        & (F.col("metadata.dominio") == "organizacion")
        & F.col("registro.id_organizacion").isNotNull()
    )
    .withColumn(
        "fecha_alta_parsed",
        F.expr("try_cast(registro.fecha_alta AS DATE)")
    )
    .withColumn(
        "fecha_baja_parsed",
        F.expr("try_cast(registro.fecha_baja AS DATE)")
    )
)

aptos = candidatos.filter(
    F.col("fecha_alta_parsed").isNotNull()
    & (
        F.col("registro.fecha_baja").isNull()
        | F.col("fecha_baja_parsed").isNotNull()
    )
)

processed = aptos.select(
    F.col("registro.id_organizacion").alias("id_organizacion"),
    F.col("metadata.jurisdiccion").alias("jurisdiccion"),
    F.col("metadata.lote_id").alias("lote_id"),
    F.col("registro.nombre").alias("nombre"),
    F.col("registro.descripcion").alias("descripcion"),
    F.col("registro.c_organizacion").alias("c_organizacion"),
    F.col("fecha_alta_parsed").alias("fecha_alta"),
    F.col("fecha_baja_parsed").alias("fecha_baja"),
    F.current_timestamp().alias("processed_at"),
)

print(f"Candidatos: {candidatos.count()}")
print(f"Descartados por tipado: {candidatos.count() - aptos.count()}")
print(f"Processed: {processed.count()}")

processed.show(20, truncate=False)

(
    processed.write
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "processed.organizacion")
    .option("user", "bnh")
    .option("password", "BnhLaboratory1234")
    .option("driver", "org.postgresql.Driver")
    .option("truncate", "true")
    .mode("overwrite")
    .save()
)

print("Carga PostgreSQL completada")

spark.stop()
