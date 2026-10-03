# DESIGN: Flink 2.2 + Kafka + faker playground (with Beam on Flink)

Status: APPROVED by owner (Gate A, 2026-10-03) · Owner: tkaymak · Date: 2026-10-03 · Revision 6 (memory-optimised; Confluent Schema Registry per owner decision)

This document is the **single source of truth**. Implement exactly what is specified. If something you need is not specified, do **not** guess. Stop and list the open question in your final answer.

---

## 1. Background and goal
- This repository is `tkaymak/sql-cli-for-apache-flink-docker`, a fork of `Aiven-Open/sql-cli-for-apache-flink-docker`. Upstream is a Flink **1.17.1** docker-compose with a Flink SQL CLI, last updated in 2023.
- It is used in the FHNW course "CAS Data Engineering – Big Data" (German-speaking students, laptops with 4–8 GB of RAM for Docker, a mix of Apple Silicon/arm64 and x86/amd64).
- Goal: a local playground where students can:
  1. generate fake data with **flink-faker**,
  2. write it to **Apache Kafka 4.1.1 (KRaft)**,
  3. query it with **Flink SQL 2.2.1** (windowing with TUMBLE/HOP/SESSION),
  4. sink results into **PostgreSQL**,
  5. use **Avro with a Schema Registry**,
  6. run **Apache Beam 2.76.0 (Python)** pipelines on the **same Flink 2.2.1 cluster**.
- Everything must work with `docker compose` on arm64 and amd64, without any local Java, Maven or Python installation.

## 2. Pinned versions (do not change; all verified to exist on 2026-10-03, multi-arch amd64+arm64)
| Component | Version / artifact |
|---|---|
| Flink image | `flink:2.2.1-scala_2.12-java17` (official tag; contains `wget` and `curl`; ships `flink-json-2.2.1.jar` and `flink-csv` in `/opt/flink/lib`; `bin/sql-client.sh embedded` exists in 2.2.1) |
| Kafka SQL connector | `https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar` |
| JDBC connector core | `https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.1.0-2.2/flink-connector-jdbc-core-4.1.0-2.2.jar` |
| JDBC Postgres dialect | `https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/4.1.0-2.2/flink-connector-jdbc-postgres-4.1.0-2.2.jar` |
| Postgres JDBC driver | `https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar` |
| Avro Confluent format | `https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/2.2.1/flink-sql-avro-confluent-registry-2.2.1.jar` |
| flink-faker | build arg `FAKER_JAR_URL`, default `https://github.com/tkaymak/flink-faker/releases/download/v0.6.0/flink-faker-0.6.0.jar` (this release is published later. For local test builds pass `--build-arg FAKER_JAR_URL=https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar`, which exists but only works with Flink 1.17 at runtime; the reviewer swaps in the real 0.6.0 jar) |
| Kafka | `apache/kafka:4.1.1` (KRaft, no ZooKeeper; CLI tools in `/opt/kafka/bin`) |
| Schema Registry | `confluentinc/cp-schema-registry:8.2.4` (the reference implementation, as discussed on the Day-2 slides; memory-trimmed via heap/JVM flags, §3.4) |
| Kafka UI | `redpandadata/console:v3.12.0` (Redpanda Console, Go; works with Apache Kafka 4.1 and Confluent-compatible schema registries; measured ~50 MB vs ~400 MB for kafbat/kafka-ui) |
| PostgreSQL | `postgres:18.6` |
| Beam Java SDK (source of the Java `boot`) | `apache/beam_java17_sdk:2.76.0` (directory `/opt/apache/beam` contains `boot`, `jars`, `options`, `LICENSE`, `NOTICE`, `third_party_licenses`) |
| Beam Flink job server jar | `https://repo1.maven.org/maven2/org/apache/beam/beam-runners-flink-2.2-job-server/2.76.0/beam-runners-flink-2.2-job-server-2.76.0.jar` (exists; Beam 2.76.0 supports Flink 2.0/2.1/2.2) |
| Beam IO expansion service jar | `https://repo1.maven.org/maven2/org/apache/beam/beam-sdks-java-io-expansion-service/2.76.0/beam-sdks-java-io-expansion-service-2.76.0.jar` |
| Beam Python package | `apache-beam==2.76.0` on `python:3.12-slim`, plus `openjdk-17-jre-headless` (the Python SDK runs in LOOPBACK mode inside `beam-client`; no separate worker-pool image) |

Elasticsearch is **removed** (there is no Flink 2.x connector).

## 3. Architecture

### 3.1 Services, profiles, ports and memory
Docker Desktop on student laptops often has only 4 GB, so optional parts use **compose profiles**. `docker compose up -d --build` starts only the core (services without a profile). Optional parts are started with `--profile <name>`. The profile `all` must enable every optional service: each optional service lists both its own profile and `all`.

| Service (= `container_name`) | Profiles | Image / build | Host port → container | `mem_limit` | Measured RSS (4 SQL jobs running) |
|---|---|---|---|---|---|
| `kafka` | (core) | `apache/kafka:4.1.1` | 29092 → 29092 | 512m | ~250 MB |
| `kafka-init` | (core) | `apache/kafka:4.1.1` | – | 256m | exits after a few seconds |
| `jobmanager` | (core) | build: context `./flink`, `image: flink-playground-flink:2.2.1` | 8081 → 8081 | 768m | ~640 MB |
| `taskmanager` | (core) | build: context `./flink`, `image: flink-playground-flink:2.2.1` | – | 1536m | ~715 MB (+~300 MB while a Beam Kafka pipeline runs: Beam's Java SDK harness is a second JVM inside this container) |
| `sql-client` | (core) | build: context `./sql-client`, `args: FAKER_JAR_URL: ${FAKER_JAR_URL:-https://github.com/tkaymak/flink-faker/releases/download/v0.6.0/flink-faker-0.6.0.jar}`, `image: flink-playground-sql-client:2.2.1` | – | 512m | ~6 MB idle (JVM only while a client session runs) |
| `console` | `ui`, `all` | `redpandadata/console:v3.12.0` | 8080 → 8080 | 128m | ~50 MB |
| `schema-registry` | `avro`, `all` | `confluentinc/cp-schema-registry:8.2.4` | 8085 → 8085 | 512m | ~300 MB (estimate with 256 MB heap; the elephant measures it) |
| `postgres` | `postgres`, `all` | `postgres:18.6` | **5433** → 5432 | 256m | ~40 MB |
| `beam-client` | `beam`, `all` | build: context `./beam`, `image: flink-playground-beam:2.76.0` | – | 1536m | idle ~50 MB; while a pipeline is submitted/running ~0.6–1 GB (Python + Beam job-server JVM + short-lived expansion-service JVM) |

- **Memory (measured on Docker Desktop, arm64, 2026-10-03):** core with 4 concurrent streaming SQL jobs ≈ **1.6 GB** (before tuning: ≈ 2.0 GB). Modules are switched on only when needed:
  - `ui` +50 MB
  - `avro` +~300 MB
  - `postgres` +40 MB
  - `beam` +0.6–1 GB while a pipeline runs

  `--profile all` fits a 4 GB Docker host, but 6 GB is recommended when Beam is used together with several SQL jobs. `mem_limit` is an upper bound per container (OOM protection), not a reservation.
- Learners switch modules on/off with profiles: `docker compose --profile ui up -d` starts the UI, and `docker compose stop console` frees its RAM again. A default selection can be put into `.env` as `COMPOSE_PROFILES=ui,avro`. Provide **`.env.example`** (exact content):
  ```
  # Optionale Module, kommagetrennt: ui, avro, postgres, beam (oder all)
  # Kopieren nach .env und anpassen, z.B.: COMPOSE_PROFILES=ui
  COMPOSE_PROFILES=
  ```
- `jobmanager` and `taskmanager` use the **same** build (`./flink`), and you must give that image the name `flink-playground-flink:2.2.1` via `image:` on both services so it is built only once.
- Do **not** use the obsolete top-level `version:` key. Do not set `platform:`.
- All services except `beam-client` share the default compose network (service names are DNS names). `beam-client` joins the TaskManager's network namespace (§3.6) and can still resolve all service names.
- Postgres uses host port 5433 to avoid a clash with Airflow's Postgres from course day 1.

### 3.2 Flink configuration (Flink 2.x uses `config.yaml`, NOT `flink-conf.yaml`)
- The official image entrypoint `/docker-entrypoint.sh` applies the env var `FLINK_PROPERTIES` (YAML lines `key: value`) to `/opt/flink/conf/config.yaml` **for every command**, including arbitrary commands such as `sleep infinity`.
- Configure `jobmanager`, `taskmanager` and `sql-client` exclusively through `FLINK_PROPERTIES` (use a YAML block scalar `|`). Do not ship any Flink config file.
- `jobmanager` (`command: jobmanager`):
  ```
  jobmanager.rpc.address: jobmanager
  jobmanager.memory.process.size: 512m
  jobmanager.memory.jvm-metaspace.size: 128m
  jobmanager.memory.off-heap.size: 64m
  jobmanager.memory.jvm-overhead.min: 64m
  jobmanager.memory.jvm-overhead.max: 64m
  parallelism.default: 1
  env.java.opts.all: -XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=48m -Xss512k
  ```
- `taskmanager` (`command: taskmanager`):
  ```
  jobmanager.rpc.address: jobmanager
  taskmanager.numberOfTaskSlots: 8
  taskmanager.memory.process.size: 896m
  taskmanager.memory.managed.size: 96m
  taskmanager.memory.framework.heap.size: 64m
  taskmanager.memory.framework.off-heap.size: 32m
  taskmanager.memory.network.min: 48m
  taskmanager.memory.network.max: 64m
  taskmanager.memory.jvm-metaspace.size: 160m
  taskmanager.memory.jvm-overhead.min: 64m
  taskmanager.memory.jvm-overhead.max: 64m
  parallelism.default: 1
  env.java.opts.all: -XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=48m -Xss512k
  ```
- Why these values (measured, do not "optimise" further): `taskmanager.memory.managed.size` must **not** be 0. With 0, Flink SQL HOP and SESSION window aggregations fail with `NullPointerException: Initial Segment may not be null` (verified). 96m is enough for the course workload. JVM flags: serial GC, C1-only JIT and a capped code cache, because throughput is irrelevant at 5 rows/s.
- `sql-client` (`command: ["sleep", "infinity"]`):
  ```
  jobmanager.rpc.address: jobmanager
  rest.address: jobmanager
  rest.port: 8081
  parallelism.default: 1
  env.java.opts.client: -XX:+UseSerialGC -XX:TieredStopAtLevel=1 -Xmx256m
  ```
- `parallelism.default: 1` makes every SQL job use exactly 1 of the 8 slots, so several examples (and Beam jobs, which use `--parallelism=1`) can run at the same time.

### 3.3 Kafka (copy of the course's day-2 lab config) and topic creation
`kafka` environment (KRaft combined mode, single node):
```
KAFKA_NODE_ID: 1
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:29092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
KAFKA_HEAP_OPTS: "-Xmx256m -Xms256m"
KAFKA_JVM_PERFORMANCE_OPTS: "-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=32m"
KAFKA_NUM_NETWORK_THREADS: 2
KAFKA_NUM_IO_THREADS: 2
KAFKA_LOG_CLEANER_DEDUPE_BUFFER_SIZE: 16777216
KAFKA_LOG_SEGMENT_BYTES: 16777216
KAFKA_LOG_RETENTION_HOURS: 24
CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
```
- `kafka` healthcheck: `["CMD", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"]`, interval 5s, timeout 10s, retries 20, start_period 20s.
- `kafka-init` is a one-shot container that pre-creates the topics, so Flink and Beam sources never see a missing topic.
  - `depends_on: kafka: condition: service_healthy` and `restart: "no"`.
  - `entrypoint: ["/bin/bash", "-c"]`, with this command (one string):
    ```
    for t in orders orders_avro beam_product_counts; do /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1; done
    ```
- Inside the network, everything uses `kafka:9092`. From the host, `localhost:29092`.

### 3.4 Other services and dependencies
- **jobmanager**: `depends_on: kafka-init: condition: service_completed_successfully`.
- **taskmanager**: `depends_on: jobmanager: condition: service_started`.
- **sql-client**:
  - `depends_on: jobmanager: condition: service_started` and `kafka-init: condition: service_completed_successfully`.
  - Volume `./examples:/opt/sql-client/examples:ro`.
- **schema-registry** (Confluent):
  - Environment (exact):
    ```
    SCHEMA_REGISTRY_HOST_NAME: schema-registry
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8085
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:9092
    SCHEMA_REGISTRY_HEAP_OPTS: "-Xms128m -Xmx256m"
    SCHEMA_REGISTRY_JVM_PERFORMANCE_OPTS: "-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=32m"
    ```
  - `depends_on: kafka: condition: service_healthy`.
- **console** (Redpanda Console):
  - env `KAFKA_BROKERS: kafka:9092`, `SCHEMAREGISTRY_ENABLED: "true"`, `SCHEMAREGISTRY_URLS: http://schema-registry:8085`.
  - `depends_on: kafka: condition: service_healthy` (NOT on schema-registry). Without the `avro` profile, only the schema view shows an error, and the UI keeps running.
- **postgres**:
  - env `POSTGRES_USER: flink`, `POSTGRES_PASSWORD: flink`, `POSTGRES_DB: playground`.
  - Volume `./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro`.
  - Healthcheck `["CMD-SHELL", "pg_isready -U flink -d playground"]`, interval 5s, timeout 5s, retries 20, start_period 10s.
  - `command: ["postgres", "-c", "shared_buffers=32MB", "-c", "max_connections=20", "-c", "work_mem=2MB"]`.
- **beam-client**: see §3.6 (`network_mode: "service:taskmanager"`, `depends_on: taskmanager: condition: service_started`, volume `./beam/data:/data`, `command: ["sleep", "infinity"]`).

### 3.5 Images to build
**`flink/Dockerfile`** (exact content):
```dockerfile
FROM flink:2.2.1-scala_2.12-java17
USER root
# Beam Java SDK harness ("boot" + jars) at its original path, so that Beam's cross-language
# Java transforms (KafkaIO) can run inside the TaskManager with environment type PROCESS.
COPY --from=apache/beam_java17_sdk:2.76.0 /opt/apache/beam /opt/apache/beam
RUN chmod -R a+rX /opt/apache/beam && chmod a+rx /opt/apache/beam/boot
USER flink
```
- Note (G2 finding, accepted by the elephant): the base image's default user is `flink` (uid 9999), so both Dockerfiles switch to `USER root` for the build steps and back to `USER flink` at the end. The `sql-client` Dockerfile also `chown`s `/opt/sql-client` to `flink:flink`.

**`sql-client/Dockerfile`** (build context `./sql-client`):
- `FROM flink:2.2.1-scala_2.12-java17`, then `ARG FAKER_JAR_URL=<default from §2>`.
- One `RUN set -eux; mkdir -p /opt/sql-client/lib; cd /opt/sql-client/lib; wget -q <url> ...` that downloads the 6 jars of §2 (Kafka, JDBC core, JDBC Postgres, Postgres driver, Avro Confluent, faker via `"${FAKER_JAR_URL}"`).
- `COPY bin/sql-client.sh /opt/sql-client/sql-client.sh`, then `RUN chmod +x /opt/sql-client/sql-client.sh`.
- `ENV SQL_CLIENT_HOME=/opt/sql-client` and `WORKDIR /opt/sql-client`.
- Verified on 2026-10-03: jars passed to the SQL client with `-l`/`-j` are shipped with each job to the session cluster (Kafka SQL jobs ran on the TaskManager without the jar in `/opt/flink/lib`). Do not copy connector jars into the Flink images.
- Do **not** override `ENTRYPOINT` and do **not** copy `examples/`; examples arrive only via the bind mount (§3.4).

**`sql-client/bin/sql-client.sh`** (exact content):
```bash
#!/bin/bash
# Starts the Flink SQL client against the jobmanager; all connector jars are shipped with each job.
exec "${FLINK_HOME}/bin/sql-client.sh" embedded -l "${SQL_CLIENT_HOME}/lib" "$@"
```

**`beam/Dockerfile`** (build context `./beam`, used by `beam-client`):
- `FROM python:3.12-slim`.
- `RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless wget ca-certificates && rm -rf /var/lib/apt/lists/*`.
- `RUN pip install --no-cache-dir apache-beam==2.76.0`.
- `RUN set -eux; mkdir -p /opt/beam/jars; cd /opt/beam/jars; wget -q <job-server jar URL from §2>; wget -q <expansion jar URL from §2>`. Keep the original Maven file names (`beam-runners-flink-2.2-job-server-2.76.0.jar`, `beam-sdks-java-io-expansion-service-2.76.0.jar`); §3.6 relies on them.
- `COPY pipelines/ /opt/beam/pipelines/`, `WORKDIR /opt/beam/pipelines`, `CMD ["sleep", "infinity"]`.

**Files to delete (G2):**
- `sql-client/conf/flink-conf.yaml` (and the then-empty `sql-client/conf/`)
- `sql-client/docker-entrypoint.sh`
- `bin/sql-client.sh` (and the then-empty `bin/`)
- `data/test.csv` (and `data/`)
- `settings/README.md` (and `settings/`)

Do **not** touch the empty directory `flink-sql-cli-docker`; it is a git submodule entry that the reviewer removes with git.

### 3.6 Beam on the shared Flink cluster (lean setup: LOOPBACK + on-demand expansion)
- There is no permanent worker-pool or expansion-service container. Everything Beam needs runs inside **`beam-client`**, and only while a pipeline runs:
  - The pipeline's Python process doubles as the **Python SDK worker** (`--environment_type=LOOPBACK`).
  - The Python FlinkRunner starts the **Flink job-server JVM** from the jar (`--flink_job_server_jar`).
  - `ReadFromKafka`/`WriteToKafka` start a **short-lived Java expansion-service JVM** from the IO expansion jar (`JavaJarExpansionService`) during pipeline construction.
- LOOPBACK means the TaskManager connects back to the worker at `localhost:<port>`. Therefore `beam-client` uses **`network_mode: "service:taskmanager"`**, sharing the TaskManager's network stack.
  - Consequences: no `ports:` and no `hostname:` on `beam-client`. It still resolves `jobmanager` and `kafka`. If the TaskManager is restarted, restart `beam-client` too (document this in the README troubleshooting).
- **Java cross-language transforms** (KafkaIO) run inside the TaskManager via environment type `PROCESS` using `/opt/apache/beam/boot` (provided by `flink/Dockerfile`).
- File IO (`ReadFromText`/`WriteToText`) runs in the LOOPBACK worker, i.e. in `beam-client`, which has `./beam/data` mounted at `/data`. `WriteToText` creates missing parent directories itself, so do not create `beam/data/output/`.
- **`beam/pipelines/common.py`** defines:
  ```python
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
      # Starts the Java IO expansion service on demand; expanded Kafka transforms run in the
      # TaskManager as PROCESS environment using the Beam Java SDK "boot" binary.
      return JavaJarExpansionService(
          "/opt/beam/jars/beam-sdks-java-io-expansion-service-2.76.0.jar",
          extra_args=[
              "{{PORT}}",
              "--javaClassLookupAllowlistFile=*",
              "--defaultEnvironmentType=PROCESS",
              '--defaultEnvironmentConfig={"command": "/opt/apache/beam/boot"}',
              "--experiments=use_deprecated_read",
          ],
      )
  ```
  (`from apache_beam.options.pipeline_options import PipelineOptions`, `from apache_beam.transforms.external import JavaJarExpansionService`.) `{{PORT}}` is Beam's placeholder for the port it picks.
- **Code structure of both pipeline scripts:**
  - Standard imports (`import json`, `import re`, `import typing`, `import apache_beam as beam`, `from apache_beam.transforms import window`, `from apache_beam.io.kafka import ReadFromKafka, WriteToKafka`, `from common import flink_options, kafka_expansion_service`, …).
  - A `run()` function containing `with beam.Pipeline(options=...) as p:` and `if __name__ == "__main__": run()`. The context manager blocks until the job ends. For the streaming script that is "forever", and the LOOPBACK worker lives in this process, so it must keep running (started with `docker compose exec -d`).
  - The DoFn for formatting implements `process(self, element, window=beam.DoFn.WindowParam)`.
  - Keep the regex `[A-Za-z']+` exactly (the input has no umlauts).
- **`beam/pipelines/wordcount.py`** (batch, `flink_options(streaming=False)`):
  - Reads `/data/input.txt`, splits into words (regex `[A-Za-z']+`), lower-cases them and counts.
  - Writes `/data/output/wordcount` via `WriteToText(..., num_shards=1)` with lines formatted as `f"{word}: {count}"`.
  - `beam/data/input.txt` contains exactly these 2 lines, repeated 3 times (6 lines total):
    ```
    Ein Stream ist eine Tabelle in Bewegung
    Batch ist nur ein spezieller Fall von Streaming
    ```
  - Expected: `beam/data/output/wordcount-00000-of-00001` contains the line `ist: 6`.
- **`beam/pipelines/kafka_window_count.py`** (streaming, `flink_options(streaming=True, extra_args=["--experiments=use_deprecated_read"])`):
  - `ReadFromKafka(consumer_config={"bootstrap.servers": "kafka:9092", "auto.offset.reset": "earliest", "group.id": "beam-window-count"}, topics=["orders"], expansion_service=kafka_expansion_service())`.
    - Element timestamps are those assigned by KafkaIO (**processing time**). Do not re-assign event timestamps; document this in a German comment as a simplification.
  - Then this chain: `beam.Map(lambda kv: json.loads(kv[1])["product"])` (the element is a `(key, value)` tuple of bytes) → `beam.WindowInto(window.FixedWindows(60))` → `beam.Map(lambda p: (p, 1))` → `beam.CombinePerKey(sum)`.
  - Then a `DoFn` with `window=beam.DoFn.WindowParam` that emits `(product.encode(), json.dumps({"product": product, "count": count, "window_end": window.end.to_utc_datetime().isoformat()}).encode())`, then `.with_output_types(typing.Tuple[bytes, bytes])`.
  - Then `WriteToKafka(producer_config={"bootstrap.servers": "kafka:9092"}, topic="beam_product_counts", expansion_service=kafka_expansion_service())`.
  - Precondition: SQL example 02 (§4) is running, so `orders` gets data.
  - Expected: messages in topic `beam_product_counts` within ~2 minutes.
- **Fallback (only if the elephant's verification shows LOOPBACK does not work):** a Python worker-pool sidecar (`apache/beam_python3.12_sdk:2.76.0`, `--worker_pool`, `network_mode: service:taskmanager`, `--environment_type=EXTERNAL --environment_config=localhost:50000`). Implementers do **not** build this unless a task card says so.

## 4. Flink SQL examples (`examples/`) and helpers
- Every `.sql` file (01–05 without exception) begins with the two `SET` lines below. Statements described per example come after them.
- Content that is left to you: the wording of the German comments (1–3 lines per statement explaining what it demonstrates) and the README prose. Write it yourself, concise and correct.
- Each `.sql` file is self-contained (it creates its own tables with `CREATE TEMPORARY TABLE`), has German `--` comments explaining what it shows, and starts with:
  ```sql
  SET 'sql-client.execution.result-mode' = 'tableau';
  SET 'execution.runtime-mode' = 'streaming';
  ```
- `run-example.sh` takes a path **relative to the repo root that starts with `examples/`**, e.g. `./run-example.sh examples/01_faker_basics.sql`.

**Block A, the faker table `orders_faker`** (verbatim wherever needed):
```sql
CREATE TEMPORARY TABLE orders_faker (
  order_id   STRING,
  customer   STRING,
  product    STRING,
  amount     DOUBLE,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '5',
  'fields.order_id.expression'   = '#{Internet.uuid}',
  'fields.customer.expression'   = '#{Name.firstName}',
  'fields.product.expression'    = '#{Options.option ''Laptop'',''Phone'',''Tablet'',''Monitor'',''Keyboard''}',
  'fields.amount.expression'     = '#{Number.randomDouble ''2'',''5'',''500''}',
  'fields.order_time.expression' = '#{date.past ''5'',''SECONDS''}'
);
```
**Block B, the Kafka JSON table `orders_kafka`** (verbatim wherever needed):
```sql
CREATE TEMPORARY TABLE orders_kafka (
  order_id STRING, customer STRING, product STRING, amount DOUBLE, order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-playground',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

**`01_faker_basics.sql`**: Block A, then `SELECT * FROM orders_faker LIMIT 10;`. Expected: 10 rows, then the script ends.

**`02_faker_to_kafka_json.sql`**: Block A, Block B, then `INSERT INTO orders_kafka SELECT * FROM orders_faker;`. Expected: the script ends after submitting, the job stays RUNNING in the Flink UI, and topic `orders` receives JSON messages.

**`03_windows_tumble_hop_session.sql`** (requires 02 running): `SET 'table.exec.source.idle-timeout' = '5 s';`, Block B, then exactly these three queries:
```sql
SELECT window_start, window_end, product, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
FROM TABLE(TUMBLE(TABLE orders_kafka, DESCRIPTOR(order_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, product
LIMIT 10;

SELECT window_start, window_end, product, COUNT(*) AS orders
FROM TABLE(HOP(TABLE orders_kafka, DESCRIPTOR(order_time), INTERVAL '5' SECOND, INTERVAL '20' SECOND))
GROUP BY window_start, window_end, product
LIMIT 10;

SELECT window_start, window_end, customer, COUNT(*) AS orders
FROM TABLE(SESSION(TABLE orders_kafka PARTITION BY customer, DESCRIPTOR(order_time), INTERVAL '5' SECOND))
GROUP BY window_start, window_end, customer
LIMIT 10;
```
Expected: three result tables printed, one after another. In tableau result mode, a streaming `SELECT ... LIMIT n` stops after n rows and the client continues with the next statement. The reviewer verifies this; do not restructure the queries.

**`04_kafka_to_postgres.sql`** (requires `--profile postgres` and 02 running): Block B, then
```sql
CREATE TEMPORARY TABLE product_revenue (
  window_start TIMESTAMP(3), window_end TIMESTAMP(3), product STRING, orders BIGINT, revenue DOUBLE,
  PRIMARY KEY (window_start, product) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/playground',
  'table-name' = 'product_revenue',
  'username' = 'flink',
  'password' = 'flink'
);

INSERT INTO product_revenue
SELECT window_start, window_end, product, COUNT(*) AS orders, SUM(amount) AS revenue
FROM TABLE(TUMBLE(TABLE orders_kafka, DESCRIPTOR(order_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, product;
```
Expected: after ~1–2 min, `docker compose exec postgres psql -U flink -d playground -c "SELECT count(*) FROM product_revenue;"` returns > 0.

**`05_avro_schema_registry.sql`** (requires `--profile avro`): Block A, then
```sql
CREATE TEMPORARY TABLE orders_avro (
  order_id STRING, customer STRING, product STRING, amount DOUBLE, order_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_avro',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-playground-avro',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8085'
);

INSERT INTO orders_avro SELECT order_id, customer, product, amount, order_time FROM orders_faker;

SELECT * FROM orders_avro LIMIT 5;
```
Expected: subject `orders_avro-value` exists (`curl -s localhost:8085/subjects`) and 5 rows are printed. The INSERT job keeps running.

**`postgres/init.sql`** (exact):
```sql
CREATE TABLE IF NOT EXISTS product_revenue (
  window_start TIMESTAMP NOT NULL,
  window_end   TIMESTAMP NOT NULL,
  product      VARCHAR(64) NOT NULL,
  orders       BIGINT,
  revenue      DOUBLE PRECISION,
  PRIMARY KEY (window_start, product)
);
```

**`run-example.sh`** (repo root, executable, exact):
```bash
#!/bin/bash
# Usage: ./run-example.sh examples/01_faker_basics.sql
set -euo pipefail
case "${1:-}" in
  examples/*.sql) ;;
  *) echo "Usage: $0 examples/<file>.sql" >&2; exit 1 ;;
esac
docker compose exec -T sql-client /opt/sql-client/sql-client.sh -f "/opt/sql-client/${1}"
```
Interactive use: `docker compose exec sql-client /opt/sql-client/sql-client.sh`.

**`.gitignore`** (exact content, replacing the old one):
```
.DS_Store
beam/data/output/
```

## 5. README.md (rewrite)
- English. Title "Flink SQL & Beam Playground (Flink 2.2, Kafka 4, faker)". Sections:
  1. What's inside (services table with profiles and ports)
  2. Requirements (Docker Desktop with Compose v2; Docker memory: 4 GB is enough for core and the light modules; 6 GB recommended with Beam). Include the measured RAM per module from the §3.1 table.
  3. Quickstart
  4. Modules / profiles: switching on and off, `docker compose stop <service>` to free RAM, `.env` with `COMPOSE_PROFILES`
  5. Flink SQL examples (from §4, incl. preconditions)
  6. Beam on Flink (how to run both pipelines, e.g. `docker compose --profile beam up -d --build`, then `docker compose exec beam-client python wordcount.py`)
  7. Troubleshooting (ports in use, memory, `docker compose --profile all down -v`)
  8. Credits & license: fork of Aiven-Open/sql-cli-for-apache-flink-docker (Apache 2.0); flink-faker by Konstantin Knauf (knaufk), Flink 2.x port by truelzsch, packaged by tkaymak; Confluent Schema Registry (Confluent Community License) as Schema Registry; Redpanda Console (Redpanda Data, BSL/community edition) as Kafka UI; Apache Flink, Kafka and Beam are trademarks of the ASF.
- After that, a German section **"Kurs-Quickstart (CAS Data Engineering, FHNW)"** with numbered steps:
  `git clone` → `docker compose up -d --build` (core ≈ 1.6 GB RAM) → open Flink UI → `./run-example.sh examples/01_faker_basics.sql` → start 02 → run 03 → optional profiles `ui`, `postgres`, `avro`, `beam` → `docker compose --profile all down -v`.
- Remove the old reference to `img/flink-web-ui.png` (it shows Flink 1.x) and delete the `img/` folder.

## 6. Work packages (the elephant hands out one task card per package)
- **G2** – `flink/Dockerfile`, `sql-client/Dockerfile`, `sql-client/bin/sql-client.sh`, and the deletions in §3.5.
- **G3** – `docker-compose.yml` (§3.1–3.4, §3.6), `postgres/init.sql`, `.gitignore` (§4), `.env.example` (§3.1).
- **G4** – `examples/01…05` and `run-example.sh` (§4).
- **G5** – `beam/Dockerfile`, `beam/pipelines/common.py`, `wordcount.py`, `kafka_window_count.py`, `beam/data/input.txt` (§3.5, §3.6, lean LOOPBACK setup).
- **G6** – `README.md` and deletion of `img/` (§5).

## 7. Rules for implementers
- Only create, modify or delete files inside this repository, and only those named in your task card.
- Files not mentioned anywhere in this document (`LICENSE`, `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`, `.github/**`) stay unchanged.
- **Never run git commands** (not even read-only ones).
- Do not change pinned versions, ports, service names or file paths.
- You may run `docker compose config` (syntax check) and `docker build` for the Dockerfiles of your own work package. Do **not** run `docker compose up` (the elephant verifies end to end).
- When done, list every file you created, changed or deleted, and every deviation from or open question about this document.

## 8. Acceptance (verified by the elephant, end to end)
1. `docker compose config -q` and `docker compose --profile all config -q` succeed.
2. `docker compose up -d --build` (on a Docker host with 4 GB; core RSS ≤ 1.8 GB with examples 02+03 running, checked with `docker stats`):
   - jobmanager, taskmanager, sql-client and kafka are running/healthy, and kafka-init exited 0, within 3 min.
   - The Flink UI at http://localhost:8081 shows 1 TaskManager with 8 slots.
3. Examples 01–05 behave as described in §4 (with their stated preconditions/profiles).
4. `--profile ui`: Redpanda Console at http://localhost:8080 lists the topics `orders`, `orders_avro` and `beam_product_counts` (pre-created by kafka-init). With `avro` also active, it shows the subject `orders_avro-value`. With only `--profile ui`, the console container stays running.
5. `--profile beam`:
   - `docker compose exec beam-client python wordcount.py` finishes, and `beam/data/output/wordcount-00000-of-00001` contains `ist: 6`. `docker stats` shows beam-client's peak RSS.
   - `docker compose exec -d beam-client python kafka_window_count.py` produces messages in `beam_product_counts`.
   - Both jobs are visible in the Flink UI.
6. `docker compose --profile all down -v` removes everything.
