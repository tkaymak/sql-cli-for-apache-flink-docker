FROM flink:2.2.1-scala_2.12-java17
USER root
# Beam Java SDK harness ("boot" + jars) at its original path, so that Beam's cross-language
# Java transforms (KafkaIO) can run inside the TaskManager with environment type PROCESS.
COPY --from=apache/beam_java17_sdk:2.76.0 /opt/apache/beam /opt/apache/beam
RUN chmod -R a+rX /opt/apache/beam && chmod a+rx /opt/apache/beam/boot
USER flink
