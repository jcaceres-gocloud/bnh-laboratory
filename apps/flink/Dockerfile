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
