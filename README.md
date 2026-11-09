# hospedaplus-pipeline

Pipeline de datos de HospedaPlus: Airflow 3 (CeleryExecutor) + RabbitMQ + Spark → lakehouse S3 (Bronze, Silver, Gold).

## Arquitectura

| Rol | Servicios (docker compose) | En AWS |
|---|---|---|
| Master | `airflow-apiserver` (UI :8080), `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `rabbitmq` (UI :15672), `flower` (:5555), `postgres` (metadatos) | EC2 núcleo (t3.medium) |
| Worker bronze | `worker-bronze` → cola `bronze` | EC2 workers (t3.xlarge) |
| Worker silver | `worker-silver` → cola `silver` | EC2 workers |
| Worker gold | `worker-gold` → cola `gold` | EC2 workers |

Cada tarea elige su worker con `queue="bronze" | "silver" | "gold"`. Spark corre en modo local
dentro del worker que ejecuta la tarea, y lee/escribe S3 con `s3a://`.

Los DAGs **no** son tres (uno por capa): hay un DAG de bronze por fuente (cada una tiene su
horario) y DAGs de silver/gold por tema. Se encadenan con *Assets*: cuando un DAG de bronze
escribe, se disparan solos los de silver que lo leen.

## Arrancar (una sola máquina, para probar)

```bash
cp .env.example .env      # NEON_READ_URL (etl_reader), S3_BUCKET, credenciales AWS
                          # NEON_WRITE_URL (app_writer) solo si vas a correr gold_neon_writeback
docker compose up -d      # la primera vez construye la imagen (~5 min)
```

- Airflow: http://localhost:8080 (airflow / airflow)
- Flower: http://localhost:5555
- RabbitMQ: http://localhost:15672 (hospedaplus / hospedaplus)

Los DAGs arrancan pausados: actívalos en la UI. Ninguno se lanza a mano salvo `bronze_f1_historical`:
unos corren por reloj y el resto se dispara solo cuando llegan sus datos (Assets).
Para repetir una corrida usa **Clear** o ▶ con un *Run ID* propio (todo es idempotente).

## DAGs

Horas en UTC (Colombia = UTC−5).

### Bronze (copia cruda a S3, cola `bronze`)

| DAG | Horario | Destino |
|---|---|---|
| `bronze_neon_f2_f3` | cada hora (incremental) | `bronze/f2_ledger/…`, `bronze/f3_reservations/…`, `bronze/f3_anomalies/…` (JSON Lines) |
| `bronze_f5_reviews` | cada hora (incremental) | `bronze/f5_reviews/fecha=…/hora=HH.jsonl` (API de reseñas, `updated_from`) |
| `bronze_f7_fx` | diario 01:00 | `bronze/f7_fx/fecha=YYYY-MM-DD.json` (Frankfurter v2, base USD) |
| `bronze_f6_events` | diario 02:30 | `bronze/f6_events/fecha=YYYY-MM-DD.jsonl` (Neon `operation.local_events`) |
| `bronze_neon_full_load` | diario 03:00 | Foto completa de Neon en una transacción, con `_SUCCESS`: `bronze/f3_reservations_full/`, `f3_anomalies_full/`, `f3_inventory_full/`, `f3_relocations_full/`, `f3_rejections_full/` |
| `bronze_neon_dims` | diario 03:15 | `bronze/neon_dims/<tabla>/fecha=YYYY-MM-DD.jsonl` (hoteles, tipos de habitación, canales, políticas de cancelación, tarifas; sin huéspedes) |
| `bronze_f4_agencias` | lunes 07:00 | verifica `bronze/f4_agencias/semana=YYYY-Www/` (lo sube `f4-weekly` con `F4_S3_BUCKET`) |
| `bronze_f6_holidays` | día 1 de cada mes, 02:00 | `bronze/f6_holidays/anio=YYYY/pais=XX.json` (Nager API v4, año actual y siguiente) |
| `bronze_f1_historical` | manual, una vez | `bronze/f1_historical/hotel_bookings.csv` (Kaggle, desde hospedaplus-csv) |

### Silver (limpieza con Spark, cola `silver`)

| DAG | Se dispara con | Destino (Parquet) |
|---|---|---|
| `silver_fx_daily` | `bronze_f7_fx` | `silver/tasa_cambio_diaria/` (COP por unidad, todos los días con relleno hacia adelante) |
| `silver_reservas_daily` | (fx + full_load + dims) **o** cada copia horaria de `bronze_neon_f2_f3` | `silver/reservas/`, `silver/cancelaciones/`, `silver/reservas_rechazadas/` (montos en COP con comisión) |
| `silver_ocupacion_daily` | full_load + dims | `silver/ocupacion_diaria/` (hotel × tipo de habitación × noche) |
| `silver_calendario_daily` | (events + dims) **o** `bronze_f6_holidays` | `silver/calendario_hotel/` (hotel × día con festivo, evento y fin de semana) |
| `silver_resenas_daily` | reloj, diario 03:30 | `silver/resenas/` (última versión, sin duplicados entre plataformas, con sentimiento) |
| `silver_f1_historico` | `bronze_f1_historical` | `silver/historico_f1/` |

### Gold (cola `gold`)

| DAG | Se dispara con | Destino |
|---|---|---|
| `gold_modelo_daily` | ocupación + calendario + reservas + reseñas (una vez por noche, ~03:35) | Estrella: `gold/dim_fecha`, `dim_hotel`, `dim_canal`, `dim_tipo_habitacion`; `fact_reservas`, `fact_cancelaciones`, `fact_ocupacion_diaria`, `fact_resenas`; marts `mart_p1_cancelacion`, `mart_p1_historico_f1`, `mart_p2_sobreventa`, `mart_p3_revenue`, `mart_p3_precio_sugerido`, `mart_p4_comisiones`, `mart_p5_reputacion` |
| `gold_neon_writeback` | `gold_modelo_daily` | Neon `operation.cancellation_rate_by_lead` (rol `app_writer`, `NEON_WRITE_URL`). Con `NEON_REFRESH_OVERBOOKING=true` además llama `operation.refresh_overbooking_limits()` (apagado por defecto) |

Cada job de Spark corre los controles de `spark_jobs/quality.py` antes de escribir: si uno falla,
la tarea falla y la versión anterior queda intacta.

## Estructura

```
dags/                         DAGs (uno por archivo)
dags/hospedaplus/             lake.py (S3 y assets) y neon.py (lectura de Neon)
spark_jobs/                   jobs silver/gold, quality.py (controles) y session.py (SparkSession con s3a)
tests/                        validación de DAGs y pruebas de los jobs Spark (pytest)
deploy/nginx/                 Nginx del núcleo (subdominios *-buenavista.coderhivex.com)
docs/                         despliegue en AWS y CI/CD
Dockerfile                    apache/airflow:3.1.5 + Java 17 + pyspark + jars hadoop-aws
docker-compose.yaml           todo en una máquina (master + 3 workers + RabbitMQ + Flower)
docker-compose.nucleo.yaml    EC2 núcleo
docker-compose.workers.yaml   EC2 workers
```

## Despliegue en AWS (2 EC2)

`docker-compose.nucleo.yaml` (EC2 núcleo: Airflow + RabbitMQ) y `docker-compose.workers.yaml`
(EC2 workers: 3 workers Celery + Spark). Guía paso a paso en [docs/despliegue_aws.md](docs/despliegue_aws.md).

Las UIs del núcleo se publican con Nginx ([deploy/nginx/hospedaplus.conf](deploy/nginx/hospedaplus.conf)) y
certificado de origen de Cloudflare:

- Airflow: https://airflow-buenavista.coderhivex.com
- Flower: https://flower-buenavista.coderhivex.com
- RabbitMQ: https://rabbitmq-buenavista.coderhivex.com

En el núcleo Flower y RabbitMQ solo escuchan en `127.0.0.1`; Postgres (5432), AMQP (5672) y la API de
ejecución (8080) quedan abiertos solo al security group de los workers.

CI/CD (lint, validación de DAGs, pruebas Spark, calidad de datos, imagen versionada en GHCR y despliegue opcional): [docs/ci_cd.md](docs/ci_cd.md).
