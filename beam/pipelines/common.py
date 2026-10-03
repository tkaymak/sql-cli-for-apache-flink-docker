from __future__ import annotations

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.external import JavaJarExpansionService

BASE_ARGS = [
    "--runner=FlinkRunner",
    "--flink_master=jobmanager:8081",
    "--flink_version=2.2",
    "--flink_job_server_jar=/opt/beam/jars/beam-runners-flink-2.2-job-server-2.76.0.jar",
    "--environment_type=LOOPBACK",
    "--parallelism=1",
]


def flink_options(streaming: bool, extra_args: list[str] | None = None) -> PipelineOptions:
    return PipelineOptions(BASE_ARGS + (["--streaming"] if streaming else []) + (extra_args or []))


def kafka_expansion_service() -> JavaJarExpansionService:
    # Starts a slim Java expansion service on demand (only while the pipeline is built).
    # The expanded Kafka transforms run EMBEDDED inside the Flink TaskManager JVM.
    return JavaJarExpansionService(
        "/opt/beam/jars/beam-sdks-java-expansion-service-app-2.76.0.jar",
        classpath=["/opt/beam/jars/kafka/*.jar"],
        extra_args=[
            "{{PORT}}",
            "--javaClassLookupAllowlistFile=*",
            "--defaultEnvironmentType=EMBEDDED",
            "--experiments=use_deprecated_read",
        ],
    )
