FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir apache-beam==2.76.0

RUN set -eux; \
    mkdir -p /opt/beam/jars/kafka; \
    cd /opt/beam/jars; \
    wget -q https://repo1.maven.org/maven2/org/apache/beam/beam-runners-flink-2.2-job-server/2.76.0/beam-runners-flink-2.2-job-server-2.76.0.jar; \
    wget -q https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-expansion-service-app/2.76.0/beam-sdks-java-expansion-service-app-2.76.0.jar; \
    cd kafka; \
    wget -q https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-io-kafka/2.76.0/beam-sdks-java-io-kafka-2.76.0.jar; \
    wget -q https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-extensions-avro/2.76.0/beam-sdks-java-extensions-avro-2.76.0.jar; \
    wget -q https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-extensions-protobuf/2.76.0/beam-sdks-java-extensions-protobuf-2.76.0.jar; \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar; \
    wget -q https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.6-4/zstd-jni-1.5.6-4.jar; \
    wget -q https://repo1.maven.org/maven2/at/yawk/lz4/lz4-java/1.10.1/lz4-java-1.10.1.jar; \
    wget -q https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar; \
    wget -q https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/4.33.2/protobuf-java-4.33.2.jar

COPY pipelines/ /opt/beam/pipelines/

WORKDIR /opt/beam/pipelines

CMD ["sleep", "infinity"]
