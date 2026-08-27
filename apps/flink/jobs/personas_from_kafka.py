from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:19092")
        .set_topics("bnh.personas")
        .set_group_id("bnh-flink-personas-smoke")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    personas = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka - bnh.personas",
    )

    personas.print()

    env.execute("BNH - Personas Kafka Smoke")


if __name__ == "__main__":
    main()
