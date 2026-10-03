FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir apache-beam==2.76.0

RUN set -eux; \
    mkdir -p /opt/beam/jars; \
    cd /opt/beam/jars; \
    wget -q https://repo1.maven.org/maven2/org/apache/beam/beam-runners-flink-2.2-job-server/2.76.0/beam-runners-flink-2.2-job-server-2.76.0.jar; \
    wget -q https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-io-expansion-service/2.76.0/beam-sdks-java-io-expansion-service-2.76.0.jar

COPY pipelines/ /opt/beam/pipelines/

WORKDIR /opt/beam/pipelines

CMD ["sleep", "infinity"]
