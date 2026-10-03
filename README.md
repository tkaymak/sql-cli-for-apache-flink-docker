# Flink SQL & Beam Playground (Flink 2.2, Kafka 4, faker)

A lean, containerised streaming and batch data engineering playground built for hands-on learning and prototyping. It bundles **Apache Flink 2.2.1**, **Apache Kafka 4.1.1 (KRaft)**, **flink-faker**, **Confluent Schema Registry**, **PostgreSQL**, **Redpanda Console**, and **Apache Beam 2.76.0 (Python on Flink)**.

Everything runs locally via Docker Compose on both **Apple Silicon (arm64)** and **Intel/AMD (amd64)** without requiring local Java, Maven, or Python installations.

---

## 1. What's Inside

The environment uses Docker Compose profiles so that only the lightweight core services start by default. Optional components are loaded only when needed.

| Service (`container_name`) | Profiles | Image / Build Context | Host Port → Container | Memory Limit (`mem_limit`) | Measured RSS | Description |
|---|---|---|---|---|---|---|
| `kafka` | *(core)* | `apache/kafka:4.1.1` | `29092:29092` | 512m | ~170–345 MB | Kafka 4.1 broker in KRaft combined mode (no ZooKeeper) |
| `kafka-init` | *(core)* | `apache/kafka:4.1.1` | – | 256m | *exits* | One-shot container pre-creating default topics (`orders`, `orders_avro`, `beam_product_counts`) |
| `jobmanager` | *(core)* | `flink:2.2.1-scala_2.12-java17` (official image, no build) | `8081:8081` | 1024m | ~610 MB | Flink 2.2.1 JobManager & Web UI (~720 MB after Beam jobs: job-server jar uploaded as blob) |
| `taskmanager` | *(core)* | `flink:2.2.1-scala_2.12-java17` (official image, no build) | – | 1536m | ~650–740 MB | Flink TaskManager with 8 slots (≈780 MB while Beam Kafka pipeline runs; KafkaIO runs embedded) |
| `sql-client` | *(core)* | `./sql-client` (`flink-playground-sql-client:2.2.1`, build arg `FAKER_JAR_URL`) | – | 512m | ~6 MB idle | Flink SQL Client pre-loaded with Kafka, JDBC Postgres, Avro, and faker connectors |
| `console` | `ui`, `all` | `redpandadata/console:v3.12.0` | `8080:8080` | 192m | ~70–115 MB | Fast Go-based Kafka and Schema Registry Web UI (Redpanda Console) |
| `schema-registry` | `avro`, `all` | `confluentinc/cp-schema-registry:8.2.4` | `8085:8085` | 512m | ~110–260 MB | Confluent Schema Registry for Avro serialization |
| `postgres` | `postgres`, `all` | `postgres:18.6` | `5433:5432` | 256m | ~20–75 MB | PostgreSQL sink database (port 5433 on host) |
| `beam-client` | `beam`, `all` | `./beam` (`flink-playground-beam:2.76.0`) | – | 1536m | ~20–40 MB idle | Apache Beam 2.76.0 client (~770–805 MB active; LOOPBACK Python worker + embedded KafkaIO in TaskManager) |

> **Network Note:** Internal inter-container communication uses service names on the Docker network (`kafka:9092`, `jobmanager:8081`, `postgres:5432`, `schema-registry:8085`). Host ports (`29092`, `8081`, `8080`, `8085`, `5433`) allow direct access from your host machine. `beam-client` joins `taskmanager`'s network namespace (`network_mode: "service:taskmanager"`).

---

## 2. Requirements

- **Docker Desktop** (or Docker Engine on Linux) with **Compose v2** (`docker compose` syntax).
- **Architecture:** Both `linux/arm64` (Apple Silicon) and `linux/amd64` (Intel/AMD) are natively supported.
- **Docker Host Memory:**
  - **4 GB RAM** assigned to Docker Desktop is sufficient for the core services and lightweight modules (`ui`, `postgres`, `avro`).
  - **6 GB RAM** is recommended when running Apache Beam pipelines alongside active Flink SQL jobs.
- **Memory Footprint Breakdown (measured on Docker Desktop, arm64, clean run 2026-10-03, Revision 8):**
  - **Core stack (with examples 02+03 running):** ≈ **1.6 GB RSS** (kafka 345 MB, jobmanager 610 MB, taskmanager 654 MB, sql-client 6 MB; optimised with SerialGC, TieredStopAtLevel=1, and trimmed heap sizes).
  - Modules switched on only when needed:
    - `ui` (`console`): **+70–115 MB**
    - `avro` (`schema-registry`): **+110–260 MB**
    - `postgres`: **+20–75 MB**
    - `beam`: **+~0.8 GB** (`beam-client`) and **+~0.1 GB** (`taskmanager`) while a pipeline runs.
  - **Everything at once (all SQL examples + both Beam pipelines):** measured ≈ **2.9 GB RSS**.
  - *Note:* `mem_limit` values are OOM protection upper bounds per container, not fixed reservations.

---

## 3. Quickstart

1. **Start the core services:**
   ```bash
   docker compose up -d --build
   ```
   *Only core services (`kafka`, `kafka-init`, `jobmanager`, `taskmanager`, `sql-client`) are started (≈ 1.6 GB RAM).*

2. **Verify running containers:**
   ```bash
   docker compose ps
   ```
   Ensure `jobmanager`, `taskmanager`, `sql-client`, and `kafka` are healthy/up, and `kafka-init` has exited successfully.

3. **Open the Flink Web UI:**
   Navigate to [http://localhost:8081](http://localhost:8081) in your browser. You will see 1 TaskManager with 8 available task slots.

4. **Launch the interactive Flink SQL CLI:**
   ```bash
   docker compose exec sql-client /opt/sql-client/sql-client.sh
   ```
   *(Enter `HELP;` for commands or `QUIT;` to exit.)*

5. **Run a SQL script non-interactively:**
   ```bash
   ./run-example.sh examples/01_faker_basics.sql
   ```

---

## 4. Modules & Profiles

Optional services are managed with Compose profiles (`ui`, `avro`, `postgres`, `beam`, or `all`).

### Starting Modules On-Demand

```bash
# Start Redpanda Console (Kafka UI) on http://localhost:8080
docker compose --profile ui up -d

# Start Confluent Schema Registry on http://localhost:8085
docker compose --profile avro up -d

# Start PostgreSQL on host port 5433
docker compose --profile postgres up -d

# Start Beam client
docker compose --profile beam up -d --build

# Or start all optional services at once
docker compose --profile all up -d --build
```

### Freeing Memory

To reclaim RAM without stopping the entire stack, stop individual containers when you are done with them:

```bash
docker compose stop console
docker compose stop schema-registry
docker compose stop postgres
docker compose stop beam-client
```

### Configuring Default Profiles via `.env`

You can persist your desired profile selection so you don't need to specify `--profile` every time.
Copy `.env.example` to `.env` and set `COMPOSE_PROFILES`:

```bash
cp .env.example .env
```

Edit `.env` (matches `.env.example`):
```ini
# Optionale Module, kommagetrennt: ui, avro, postgres, beam (oder all)
# Kopieren nach .env und anpassen, z.B.: COMPOSE_PROFILES=ui
COMPOSE_PROFILES=
```

Then simply run:
```bash
docker compose up -d
```

---

## 5. Flink SQL Examples

The repository includes five pre-configured SQL examples under `examples/`. Execute them using the `./run-example.sh` helper script:

```bash
./run-example.sh examples/<file>.sql
```

### Why Bounded Reads?

In Flink 2.2.1 streaming mode, running `SELECT ... LIMIT n` on an unbounded source prints *n* rows to the console, but the streaming job never terminates and the SQL client does not exit, causing automated execution scripts like `./run-example.sh` to hang. Examples 01, 03, and 05 therefore use **bounded reads** (dynamic table hints `/*+ OPTIONS(...) */` or a `LIKE ... WITH ('scan.bounded.mode' = 'latest-offset')` snapshot table) so Flink emits a final watermark at the end of input, fires all open windows, and exits cleanly. To run continuous, unbounded queries instead, start an interactive session with `docker compose exec sql-client /opt/sql-client/sql-client.sh` (e.g. querying `orders_kafka` directly) and stop it anytime with `Ctrl+C`.

### Overview of Examples

| File | Module / Precondition | Description |
|---|---|---|
| `examples/01_faker_basics.sql` | Core | Generates synthetic order data with `flink-faker` and displays 10 rows using a bounded dynamic table hint (`number-of-rows = 10`). |
| `examples/02_faker_to_kafka_json.sql` | Core | Continuously streams fake order events into Kafka topic `orders` as JSON. Remains running in Flink. |
| `examples/03_windows_tumble_hop_session.sql` | Core (requires `02` running for ~30 s) | Demonstrates Flink SQL window TVFs on event-time (`TUMBLE`, `HOP`, `SESSION`) using a bounded snapshot table. |
| `examples/04_kafka_to_postgres.sql` | `--profile postgres` (requires `02` running) | Aggregates 1-minute tumbling windows from Kafka and writes revenue metrics into PostgreSQL via JDBC. |
| `examples/05_avro_schema_registry.sql` | `--profile avro` | Streams fake order events in Confluent Avro to Kafka `orders_avro` and reads first 5 records with bounded offsets. |

### Step-by-Step Walkthrough

#### 1. Faker Basics
```bash
./run-example.sh examples/01_faker_basics.sql
```
Defines a temporary table `orders_faker` using the `faker` connector generating mock orders (UUIDs, names, products, amounts, timestamps). Reads 10 rows via dynamic table option `/*+ OPTIONS('number-of-rows' = '10') */` and exits cleanly (~3 s).

#### 2. Stream Faker to Kafka (JSON)
```bash
./run-example.sh examples/02_faker_to_kafka_json.sql
```
Submits a continuous streaming `INSERT INTO orders_kafka SELECT * FROM orders_faker;`. The command returns after submission. Open the Flink UI at [http://localhost:8081](http://localhost:8081) to see the job in `RUNNING` status. Topic `orders` now receives a continuous flow of JSON records (~5 rows/s).

#### 3. Window Aggregations (TUMBLE, HOP, SESSION)
```bash
# Precondition: Ensure example 02 has been running for at least ~30 s so Kafka contains data!
./run-example.sh examples/03_windows_tumble_hop_session.sql
```
Creates a bounded snapshot (`orders_snapshot WITH ('scan.bounded.mode' = 'latest-offset') LIKE orders_kafka;`) reading the topic from earliest offset up to query start (dynamic table hints are not permitted inside window TVF `TABLE ...` arguments). Evaluates event-time watermarks and prints 10 rows for each window type before terminating cleanly (~6 s):
- **Tumble Window:** Non-overlapping 10-second intervals aggregating order count and revenue per product.
- **Hopping Window:** 20-second windows advancing every 5 seconds.
- **Session Window:** Dynamic windows grouped per customer with a 5-second inactivity gap.

#### 4. Kafka to PostgreSQL Sink
```bash
# 1. Start PostgreSQL
docker compose --profile postgres up -d

# 2. Run the streaming aggregation pipeline (requires example 02 running)
./run-example.sh examples/04_kafka_to_postgres.sql

# 3. Verify inserted data in PostgreSQL after ~1–2 minutes:
docker compose exec postgres psql -U flink -d playground -c "SELECT count(*) FROM product_revenue;"
```

#### 5. Avro with Confluent Schema Registry
```bash
# 1. Start Schema Registry
docker compose --profile avro up -d

# 2. Run the Avro pipeline
./run-example.sh examples/05_avro_schema_registry.sql

# 3. Verify registered schema subject via curl:
curl -s http://localhost:8085/subjects
# Expected output: ["orders_avro-value"]
```
The script writes Avro records to `orders_avro` registering the schema `orders_avro-value`, reads the first 5 records of partition 0 via `/*+ OPTIONS('scan.bounded.mode' = 'specific-offsets', 'scan.bounded.specific-offsets' = 'partition:0,offset:5') */`, and exits cleanly while the `INSERT` job continues running in the background.

### Flink SQL Cookbook Compatibility

Recipes from [ververica/flink-sql-cookbook](https://github.com/ververica/flink-sql-cookbook) that only use `faker` run unchanged on this playground (verified: `01_create_table`, `04_where`, `01_group_by_window`, `03_group_by_session_window`, and `01_date_time`). Recipes needing external systems (such as the MySQL lookup join) do not run out of the box.

---

## 6. Beam on Flink

This playground includes **Apache Beam 2.76.0 (Python SDK)** executing on the shared Flink 2.2.1 cluster.

### Architecture: Lean LOOPBACK Execution

Instead of running separate worker-pool or persistent expansion-service containers, Beam uses a lightweight on-demand architecture:
- **LOOPBACK Python Worker:** The pipeline's Python process inside `beam-client` doubles as the SDK worker (`--environment_type=LOOPBACK`). There is no separate worker pool container or image.
- **Shared Network Namespace:** `beam-client` joins the TaskManager's network namespace (`network_mode: "service:taskmanager"`), allowing the TaskManager to connect directly back to the Python worker on `localhost`.
- **Embedded Job Server:** The Python FlinkRunner automatically spins up the Flink job-server JVM from the bundled jar (`beam-runners-flink-2.2-job-server-2.76.0.jar`).
- **EMBEDDED Cross-Language KafkaIO:** Java cross-language transforms (`ReadFromKafka`, `WriteToKafka`) run with environment type `EMBEDDED` directly inside the TaskManager JVM, reusing classes from the Flink job-server jar. No Java `boot` binary, no second JVM and no artifact transfer to the TaskManager are required.
- **Slim On-Demand Expansion Service:** During pipeline construction, the client starts a lightweight on-demand expansion service using the slim `beam-sdks-java-expansion-service-app-2.76.0.jar` (62 MB) alongside KafkaIO classpath jars (21 MB), avoiding the bloated 832 MB legacy IO expansion service jar.

### Running Beam Pipelines

1. **Start the Beam client service:**
   ```bash
   docker compose --profile beam up -d --build
   ```

2. **Run Batch Wordcount:**
   ```bash
   docker compose exec beam-client python wordcount.py
   ```
   - Reads input text from `/data/input.txt` (mounted from `./beam/data/input.txt`).
   - Counts words and writes sorted results to `/data/output/wordcount.txt`. (Due to a known Beam 2.76.0 / Flink 2.x runner bug where `WriteToText` fails during batch finalize with `IllegalStateException: TimestampCombiner moved element... to earlier time...`, `wordcount.py` uses `ToList()` and writes the sorted text file directly in Python.)
   - Inspect the output on your host:
     ```bash
     cat beam/data/output/wordcount.txt
     ```
     *(Verifiable: contains 13 lines, including `ist: 6`.)*

3. **Run Streaming Kafka Window Count:**
   *Precondition:* Ensure SQL example 02 is running so that the `orders` topic receives data.
   
   Launch the streaming pipeline in the background using `-d` (the Python process must stay alive as the LOOPBACK worker):
   ```bash
   docker compose exec -d beam-client python kafka_window_count.py
   ```
   - Consumes JSON order events from Kafka `orders` topic using KafkaIO (timestamps assigned by KafkaIO as processing time).
   - Calculates 60-second fixed tumbling window counts per product.
   - Writes result JSON tuples back to Kafka topic `beam_product_counts`.
   - View the running job in Flink Web UI ([http://localhost:8081](http://localhost:8081)).
   - Verify messages in `beam_product_counts` using the Kafka console consumer:
     ```bash
     docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic beam_product_counts --from-beginning
     ```
     *(Or view messages in **Redpanda Console** at [http://localhost:8080](http://localhost:8080) if `--profile ui` is running.)*

---

## 7. Troubleshooting

### Query Cancellation & TaskManager Restarts (Flink 2.2.1 Bug)

In Flink 2.2.1, cancelling a `SELECT` query right after submitting while its collect sink is still initializing can crash the TaskManager due to an NPE in `CollectSinkFunction.accumulateFinalResults`. The TaskManager restarts automatically (`restart: unless-stopped`). However, because `beam-client` shares the TaskManager's network namespace (`network_mode: "service:taskmanager"`), restarting `taskmanager` breaks the network stack of `beam-client`. If Beam is in use, recreate `beam-client` too:
```bash
docker compose up -d --force-recreate beam-client
```

### JobManager Restarts & Lost Jobs (No HA)

The playground runs a standalone session cluster without High Availability (HA) or persistent state storage. Restarting or recreating the `jobmanager` container loses all currently running jobs. Simply re-run the examples.

### Custom or Locally Built Faker JAR (`FAKER_JAR_URL`)

The `sql-client` Docker image builds with the build argument `FAKER_JAR_URL` (defaulting to the v0.6.0 release for Flink 2.2). If you build your own flink-faker JAR or test an alternate release, pass `--build-arg FAKER_JAR_URL=<url>` when building:
```bash
docker compose build --build-arg FAKER_JAR_URL=https://... sql-client
```

### Port Conflicts

If a container fails to bind to a host port, check whether another process on your host is using it:

| Port | Service | Resolution |
|---|---|---|
| `8081` | Flink Web UI / JobManager | Ensure no other Flink cluster or local web server is using 8081. |
| `29092` | Kafka Host Listener | Check for local Kafka or Docker containers binding 29092. |
| `8080` | Redpanda Console (`console`) | Check for other web apps on 8080. |
| `8085` | Confluent Schema Registry | Check for local Schema Registry instances on 8085. |
| `5433` | PostgreSQL | Mapped to host port 5433 to avoid clashes with default local PostgreSQL or Airflow on 5432. |

### Memory & Out-Of-Memory (OOM) Issues

- If TaskManager or Kafka crashes or gets killed, check Docker Desktop memory limits:
  - Open **Docker Desktop → Settings → Resources → Memory**.
  - Set at least **4 GB** (or **6 GB** when running Beam alongside multiple SQL jobs).
- Monitor live container memory consumption:
  ```bash
  docker stats
  ```
- Free RAM by stopping optional services:
  ```bash
  docker compose stop console schema-registry postgres beam-client
  ```

### Full Clean Teardown

To stop all containers across all profiles, remove networks, and wipe volumes:

```bash
docker compose --profile all down -v
```

---

## 8. Credits & License

- Forked from [Aiven-Open/sql-cli-for-apache-flink-docker](https://github.com/Aiven-Open/sql-cli-for-apache-flink-docker) under the **Apache License 2.0**.
- **flink-faker** by Konstantin Knauf ([knaufk](https://github.com/knaufk/flink-faker)), Flink 2.x port by [truelzsch](https://github.com/truelzsch), packaged by [tkaymak](https://github.com/tkaymak/flink-faker).
- **Confluent Schema Registry** (`confluentinc/cp-schema-registry`) is distributed under the [Confluent Community License](https://www.confluent.io/confluent-community-license/).
- **Redpanda Console** (`redpandadata/console`) is distributed by Redpanda Data under the Business Source License (BSL) / Community Edition.
- **Apache Flink**, **Apache Kafka**, and **Apache Beam** are trademarks or registered trademarks of the [Apache Software Foundation](https://www.apache.org/).
- **Docker** is a trademark of Docker, Inc.

---

## Kurs-Quickstart (CAS Data Engineering, FHNW)

Kompakte Schritt-für-Schritt-Anleitung für die Übungen im CAS Data Engineering:

1. **Repository klonen und ins Verzeichnis wechseln:**
   ```bash
   git clone https://github.com/tkaymak/sql-cli-for-apache-flink-docker.git
   cd sql-cli-for-apache-flink-docker
   ```

2. **Core-Services starten (Docker Desktop benötigt mind. 4 GB RAM; Core belegt ca. 1.6 GB):**
   ```bash
   docker compose up -d --build
   ```

3. **Flink Web UI aufrufen:**
   Öffne [http://localhost:8081](http://localhost:8081) im Browser. Unter *Task Managers* sollte 1 TaskManager mit 8 Task Slots angezeigt werden.

4. **Erstes SQL-Beispiel ausführen (Faker-Grundlagen):**
   ```bash
   ./run-example.sh examples/01_faker_basics.sql
   ```
   Erzeugt 10 Zufallsdatensätze mit flink-faker via begrenztem Lesen (`number-of-rows = 10`) und gibt sie tabellarisch im Terminal aus.

5. **Kontinuierlichen Datenstrom nach Kafka starten:**
   ```bash
   ./run-example.sh examples/02_faker_to_kafka_json.sql
   ```
   Sendet einen Streaming-Job ab, der kontinuierlich JSON-Datensätze in das Kafka-Topic `orders` schreibt. Der Job läuft dauerhaft im Flink-Cluster (in der Web UI unter *Running Jobs* sichtbar).

6. **Streaming-Fenster aggregieren:**
   Beispiel 02 für mindestens ca. 30 Sekunden laufen lassen, damit Daten in Kafka vorhanden sind.
   ```bash
   ./run-example.sh examples/03_windows_tumble_hop_session.sql
   ```
   Führt nacheinander TUMBLE- (10s), HOP- (20s mit 5s Slide) und SESSION-Fenster-Aggregationen (5s Lücke) auf einem begrenzten Snapshot (`latest-offset`) der Daten aus Kafka aus und beendet sich nach jeweils 10 Zeilen sauber.

7. **Optionale Module nach Bedarf hinzuschalten:**
   - **Kafka Web UI (Redpanda Console):**
     ```bash
     docker compose --profile ui up -d
     ```
     UI unter [http://localhost:8080](http://localhost:8080) öffnen, um Topics (`orders`, `orders_avro`, `beam_product_counts`) und Nachrichten im Browser zu inspizieren.
   - **PostgreSQL Sink:**
     ```bash
     docker compose --profile postgres up -d
     ./run-example.sh examples/04_kafka_to_postgres.sql
     ```
     Aggregierte Daten nach ca. 1–2 Minuten in PostgreSQL prüfen:
     ```bash
     docker compose exec postgres psql -U flink -d playground -c "SELECT count(*) FROM product_revenue;"
     ```
   - **Avro & Confluent Schema Registry:**
     ```bash
     docker compose --profile avro up -d
     ./run-example.sh examples/05_avro_schema_registry.sql
     ```
     Schema-Registrierung überprüfen:
     ```bash
     curl -s http://localhost:8085/subjects
     ```
   - **Apache Beam (Python auf Flink):**
     ```bash
     docker compose --profile beam up -d --build
     docker compose exec beam-client python wordcount.py
     cat beam/data/output/wordcount.txt
     ```
     *(Verifizierbar: enthält `ist: 6`.)*
     
     Streaming-Pipeline im Hintergrund (`-d`) ausführen (Beispiel 02 muss laufen):
     ```bash
     docker compose exec -d beam-client python kafka_window_count.py
     ```
     Ausgabe im Kafka-Topic `beam_product_counts` prüfen:
     ```bash
     docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic beam_product_counts --from-beginning
     ```

8. **Umgebung vollständig stoppen und aufräumen:**
   ```bash
   docker compose --profile all down -v
   ```
