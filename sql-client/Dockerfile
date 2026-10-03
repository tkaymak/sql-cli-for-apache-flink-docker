FROM flink:2.2.1-scala_2.12-java17

USER root

ARG FAKER_JAR_URL=https://github.com/tkaymak/flink-faker/releases/download/v0.6.0/flink-faker-0.6.0.jar

RUN set -eux; \
    mkdir -p /opt/sql-client/lib; \
    cd /opt/sql-client/lib; \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar; \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.1.0-2.2/flink-connector-jdbc-core-4.1.0-2.2.jar; \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/4.1.0-2.2/flink-connector-jdbc-postgres-4.1.0-2.2.jar; \
    wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar; \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/2.2.1/flink-sql-avro-confluent-registry-2.2.1.jar; \
    wget -q "${FAKER_JAR_URL}"; \
    chown -R flink:flink /opt/sql-client

COPY --chown=flink:flink bin/sql-client.sh /opt/sql-client/sql-client.sh
RUN chmod +x /opt/sql-client/sql-client.sh

ENV SQL_CLIENT_HOME=/opt/sql-client
WORKDIR /opt/sql-client

USER flink