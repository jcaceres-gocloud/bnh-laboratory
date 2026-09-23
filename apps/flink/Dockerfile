FROM flink:2.1.1-scala_2.12-java17 AS java-builder

USER root

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY java/ /build/java/

RUN mkdir -p /build/classes && \
    javac \
      -cp "/opt/flink/lib/*" \
      -d /build/classes \
      /build/java/ar/gob/bnh/flink/bucket/JurisdiccionBucketAssigner.java && \
    jar cf /build/bnh-flink-extensions.jar \
      -C /build/classes .


FROM flink:2.1.1-scala_2.12-java17

USER root

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev curl && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

RUN pip3 install --no-cache-dir \
    apache-flink==2.1.1

RUN curl -fL \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.1/flink-connector-kafka-5.0.0-2.1.jar \
    -o /opt/flink/lib/flink-connector-kafka-5.0.0-2.1.jar

RUN curl -fL \
    https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/4.2.0/kafka-clients-4.2.0.jar \
    -o /opt/flink/lib/kafka-clients-4.2.0.jar

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-2.1.1.jar \
        /opt/flink/plugins/s3-fs-hadoop/

COPY --from=java-builder \
    /build/bnh-flink-extensions.jar \
    /opt/flink/lib/bnh-flink-extensions.jar