# Infrastructure - Modern Data Lakehouse

This repository hosts the complete infrastructure for a state-of-th-art **Data Lakehouse**, designed to handle high-velocity streaming data (Bus GPS) and large-scale analytical workloads.

## 🏗️ Architecture Overview

The platform is built on a decoupled architecture where compute and storage scale independently. It leverages **Apache Iceberg** as the table format to provide ACID transactions and warehouse-like features on top of an object store.

The system flow and interactions are structured as follows:

*   **Data Ingestion**: Real-time bus GPS data is ingested from **Apache Kafka** into the system.
*   **Processing & Storage**:
    *   **Apache Spark** consumes data from Kafka, performs transformations, and writes it to **MinIO** (Object Storage) using the **Apache Iceberg** format.
    *   Spark also performs real-time upserts to **Redis** for low-latency serving of current bus locations.
*   **Metadata Management**:
    *   **Gravitino** serves as a unified REST Catalog, managing metadata for both Spark and Trino.
    *   It uses **PostgreSQL** as the persistent backend for catalog definitions.
*   **Data Access & Analytics**:
    *   **Trino** queries the Iceberg tables stored in MinIO for high-speed SQL analytics.
    *   **Apache Superset** connects to Trino to provide visual dashboards and data exploration.
*   **Observability**: **Prometheus** and **Grafana** monitor the health and performance of all components across the stack.

---

## 🚀 Detailed Components

### 1. Apache Spark (v3.5.5) - The Processing Hub
Spark is configured as the heavy-lifter for both batch ETL and real-time streaming pipelines.

*   **Configurations**:
    - Main configuration: [`./spark/conf/spark-defaults.conf`](./spark/conf/spark-defaults.conf)
    - Environment variables: [`./spark/.env.spark`](./spark/.env.spark)
    - Build definition: [`./spark/Dockerfile`](./spark/Dockerfile)
*   **Runtime Environment**:
    - **Image**: Custom-built on `python:3.12.3-slim-bookworm`.
    - **JVM**: OpenJDK 17.
    - **Core Jars**: Includes `iceberg-spark-runtime`, `hadoop-aws`, `aws-java-sdk-bundle`, and `kafka-clients`.
*   **Iceberg Details**:
    - **Extensions**: `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`.
    - **Catalog**: Named `catalog_iceberg`, type `rest`.
    - **IO Implementation**: `S3FileIO` is used for direct, high-performance interaction with MinIO.
*   **Streaming Capabilities**:
    - Built-in support for **Kafka** with checkpointing stored in MinIO.
*   **Observability**: Metrics exposed via Prometheus at `:4040/metrics/prometheus`.

### 2. Trino (v471) - High-Speed SQL Engine
Trino provides the "Interactive" layer of the Lakehouse.

*   **Configurations**:
    - Iceberg Catalog: [`./trino/etc/catalog/catalog_iceberg.properties`](./trino/etc/catalog/catalog_iceberg.properties)
*   **Iceberg Connector**:
    - **Catalog Type**: `rest` (pointing to Gravitino).
    - **Native S3**: Enabled (`fs.native-s3.enabled=true`).
*   **Unified View**: Schema changes in Spark are instantly reflected in Trino via the REST Catalog.

### 3. Gravitino (v1.1.0) - Unified Catalog & Iceberg REST Server
The central governance layer for the Lakehouse.

*   **Configurations**:
    - Main configuration: [`./gravitino/conf/gravitino.conf`](./gravitino/conf/gravitino.conf)
    - Build definition: [`./gravitino/Dockerfile`](./gravitino/Dockerfile)
*   **Iceberg REST Auxiliary Service**:
    - Runs on port `9001`, implementing the Iceberg REST Catalog spec.
*   **Metadata Storage**:
    - Uses a **JDBC backend** (PostgreSQL) in the `catalog_metastore_db` database.
*   **Decoupled Governance**: Manages the `s3a://iceberg/lakehouse` warehouse path consistently.

### 4. MinIO - S3-Compatible Storage
The physical storage layer for the Data Lake.

*   **Bucket Policy**: Data is stored in `iceberg/lakehouse`.
*   **Initialization**: Managed via `minio-client` in `../docker-compose.yml`.

### 5. Apache Kafka (v3.9.0)
Message queue running in KRaft mode.

- **Role**: Receives real-time bus GPS data.
- **Monitoring**: Metrics collected via `kafka-exporter`.

### 6. Redis (v7.2) - Real-time Serving
In-memory database for Latest Point data.

*   **Use Case**: Spark Streaming sinks current bus locations to Redis for millisecond-latency serving.

### 7. Observability (Prometheus & Grafana)
Full-stack monitoring and alerting.

*   **Configurations**:
    - Prometheus Config: [`./prometheus/conf/prometheus.yml`](./prometheus/conf/prometheus.yml)
*   **Visualization**: Grafana connects to Prometheus and Trino for operational and analytical dashboards.

---

## 📈 Performance & Tuning

| Component | Optimization | Purpose | Configuration Path |
| :--- | :--- | :--- | :--- |
| **Spark/Trino** | `S3FileIO` / `native-s3` | High throughput on Object Storage | `./spark/conf/spark-defaults.conf` |
| **Iceberg** | `ObjectStoreLocationProvider` | Prevents S3 hotspots | `./spark/conf/spark-defaults.conf` |
| **Kafka** | `KRaft Mode` | Simplifies infra (No Zookeeper) | `../docker-compose.yml` |
| **Postgres** | `Connection Pooling` | Handles concurrent metadata requests | `../.env` |

---

## 🛠️ Management & Monitoring

| Service | URL | Credentials |
| :--- | :--- | :--- |
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | - |
| **Trino UI** | [http://localhost:8088](http://localhost:8088) | - |
| **MinIO Console** | [http://localhost:9002](http://localhost:9002) | `minioadmin` / `minioadmin123` |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` |
| **Superset** | [http://localhost:8089](http://localhost:8089) | admin / admin |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | - |
| **Gravitino** | [http://localhost:8090](http://localhost:8090) | - |

## 🛠️ Deployment

- **Main Orchestration**: [`../docker-compose.yml`](../docker-compose.yml)
- **Global Variables**: [`../.env`](../.env)

1.  **Start Services**: `docker-compose up -d`
2.  **Verify**: `docker-compose ps`
