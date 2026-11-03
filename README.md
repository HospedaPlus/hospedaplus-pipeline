# hospedaplus-pipeline

Pipeline de datos de HospedaPlus: Airflow 3 (CeleryExecutor) + RabbitMQ + Spark → lakehouse S3 (Bronze, Silver, Gold).

## Arquitectura

| Rol | Servicios (docker compose) | En AWS |
|---|---|---|
| Master | `airflow-apiserver` (UI :8080), `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `rabbitmq` (UI :15672), `flower` (:5555), `postgres` (metadatos) | EC2 master |
| Worker bronze | `worker-bronze` → cola `bronze` | EC2 worker 1 |
| Worker silver | `worker-silver` → cola `silver` | EC2 worker 2 |
| Worker gold | `worker-gold` → cola `gold` | EC2 worker 3 |

Cada tarea elige su worker con `queue="bronze" | "silver" | "gold"`. Spark corre en modo local
dentro del worker que ejecuta la tarea, y lee/escribe S3 con `s3a://`.

Los DAGs **no** son tres (uno por capa): hay un DAG de bronze por fuente (cada una tiene su
horario) y DAGs de silver/gold por tema. Se encadenan con *Assets*: cuando un DAG de bronze
escribe, se disparan solos los de silver que lo leen.

## Arrancar

```bash
cp .env.example .env      # NEON_READ_URL (etl_reader), S3_BUCKET, credenciales AWS
docker compose up -d      # la primera vez construye la imagen (~5 min)
```

- Airflow: http://localhost:8080 (airflow / airflow)
- Flower: http://localhost:5555
- RabbitMQ: http://localhost:15672 (hospedaplus / hospedaplus)

Los DAGs arrancan pausados: actívalos en la UI.

## DAGs

| DAG | Horario | Cola | Destino |
|---|---|---|---|
| `bronze_f7_fx` | diario 01:00 UTC | bronze | `bronze/f7_fx/fecha=YYYY-MM-DD.json` (Frankfurter v2, base USD) |
| `bronze_f6_holidays` | día 1 de cada mes | bronze | `bronze/f6_holidays/anio=YYYY/pais=XX.json` (Nager API v4) |
| `bronze_f6_events` | diario 02:30 UTC | bronze | `bronze/f6_events/fecha=YYYY-MM-DD.jsonl` (Neon `operation.local_events`) |
| `bronze_f1_historical` | manual, una vez | bronze | `bronze/f1_historical/hotel_bookings.csv` (Kaggle, desde hospedaplus-csv) |
| `bronze_f4_agencias` | lunes 07:00 UTC | bronze | verifica `bronze/f4_agencias/semana=YYYY-Www/` (lo sube `f4-weekly` con `F4_S3_BUCKET`) |
| `bronze_f5_reviews` | cada hora (incremental) | bronze | `bronze/f5_reviews/fecha=…/hora=HH.jsonl` (API de reseñas, `updated_from`) |
| `bronze_neon_f2_f3` | cada hora (incremental) | bronze | `bronze/f2_ledger/…`, `bronze/f3_reservations/…`, `bronze/f3_anomalies/…` (JSON Lines) |
| `silver_fx_daily` | cuando cambia `bronze/f7_fx` | silver | `silver/tasa_cambio_diaria/` (parquet, todos los días con relleno hacia adelante) |

Pendientes: silver de reservas, reseñas, calendario y F1; gold.

## Estructura

```
dags/                  DAGs (uno por archivo) + dags/hospedaplus/lake.py (S3, assets, Neon)
spark_jobs/            jobs de Spark (session.py arma la SparkSession con s3a)
Dockerfile             apache/airflow:3.1.5 + Java 17 + pyspark + jars hadoop-aws
docker-compose.yaml    master + 3 workers + RabbitMQ + Flower
```

## Despliegue en AWS (2 EC2)

`docker-compose.nucleo.yaml` (EC2 núcleo: Airflow + RabbitMQ) y `docker-compose.workers.yaml`
(EC2 workers: 3 workers Celery + Spark). Guía paso a paso en [docs/despliegue_aws.md](docs/despliegue_aws.md).

CI/CD (lint, validación de DAGs, pruebas Spark, calidad de datos, imagen versionada en GHCR y despliegue opcional): [docs/ci_cd.md](docs/ci_cd.md).
