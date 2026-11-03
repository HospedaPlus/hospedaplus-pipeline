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
| `bronze_f6_holidays` | día 1 de cada mes | bronze | `bronze/f6_holidays/anio=YYYY/pais=XX.json` (Nager.Date) |
| `bronze_neon_f2_f3` | cada hora (incremental) | bronze | `bronze/f2_ledger/…`, `bronze/f3_reservations/…`, `bronze/f3_anomalies/…` (JSON Lines) |
| `silver_fx_daily` | cuando cambia `bronze/f7_fx` | silver | `silver/tasa_cambio_diaria/` (parquet, todos los días con relleno hacia adelante) |

Pendientes: bronze de F1, F4, F5 y eventos F6b; silver de reservas, reseñas y festivos; gold.

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
