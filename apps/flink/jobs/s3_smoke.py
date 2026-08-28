from pyflink.common import Types
from pyflink.common.serialization import Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    datos = env.from_collection(
        [
            '{"smoke_test":true,"origen":"pyflink","destino":"bronze"}',
        ],
        type_info=Types.STRING(),
    )

    sink = (
        FileSink
        .for_row_format(
            "s3://bnh-bronze/flink-smoke/",
            Encoder.simple_string_encoder(),
        )
        .build()
    )

    datos.sink_to(sink)

    env.execute("BNH - Flink S3 Smoke")


if __name__ == "__main__":
    main()
