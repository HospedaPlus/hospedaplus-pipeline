# Despliegue en AWS: EC2 núcleo + EC2 workers

Como en el diagrama de la arquitectura: **EC2 núcleo** (t3.medium) con Airflow y RabbitMQ, y
**EC2 workers** (t3.xlarge) con los 3 workers de Celery + Spark. Todo lo que extraen los DAGs
llega a S3 (`bronze/`, `silver/`, `gold/`), y los logs de las tareas a `airflow-logs/`.

```
                 ┌──────────── VPC us-east-2 ────────────┐
 Tú (Termius) ──►│ EC2 núcleo                            │
   :8080 :5555   │  api-server · scheduler · dag-proc.   │
                 │  triggerer · Postgres · RabbitMQ      │
                 │        ▲ 5432 · 5672 · 8080           │
                 │        │  (solo desde el SG workers)  │
                 │ EC2 workers                           │
                 │  worker-bronze · worker-silver · gold │──► S3 lakehouse
                 └───────────────────────────────────────┘    Neon · APIs
```

## 1. S3 e IAM

1. S3 → bucket `hospedaplus-lakehouse-<sufijo>` en **us-east-2**, acceso público bloqueado.
2. IAM → Roles → EC2 → política solo sobre ese bucket (`s3:ListBucket` en el bucket,
   `s3:GetObject/PutObject/DeleteObject` en `bucket/*`). Nómbralo `hospedaplus-pipeline-role`.
   Se asigna a **las dos** EC2 (los workers escriben datos y logs; el núcleo lee los logs).

## 2. Security groups

| SG | Entrada |
|---|---|
| `sg-nucleo` | 22, 8080, 5555, 15672 desde **tu IP** · 5432, 5672, 8080 desde **`sg-workers`** |
| `sg-workers` | 22 desde **tu IP** |

## 3. Lanzar las dos EC2

Ubuntu, misma VPC y subred, mismo key pair (`.pem`), rol `hospedaplus-pipeline-role`:

- `hospedaplus-nucleo`: t3.medium, 30 GB, `sg-nucleo`
- `hospedaplus-workers`: t3.xlarge, 50 GB, `sg-workers`

Anota la **IP privada** del núcleo (Detalles de la instancia → Dirección IPv4 privada).

## 4. Conectarte con Termius

1. Termius → **Keychain** → Import key → elige tu `.pem`.
2. **Hosts** → New host: Address = IP pública de la EC2, Username = `ubuntu`, Key = la del paso 1.
3. Repite para la otra EC2. Abre las dos en pestañas.

## 5. En cada EC2 (las dos)

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu && newgrp docker
git clone https://github.com/HospedaPlus/hospedaplus-pipeline.git
cd hospedaplus-pipeline
cp .env.example .env && nano .env
```

El `.env` es **el mismo en las dos**, con: `NEON_READ_URL` (etl_reader de production),
`S3_BUCKET`, `AWS_DEFAULT_REGION=us-east-2`, llaves de AWS **vacías** (las da el rol),
`AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET`, `POSTGRES_PASSWORD`, `RABBITMQ_PASSWORD` y
`_AIRFLOW_WWW_USER_PASSWORD` cambiados. En la EC2 workers además `NUCLEO_HOST=<IP privada del núcleo>`.

## 6. Arrancar

```bash
# EC2 núcleo (primero)
docker compose -f docker-compose.nucleo.yaml up -d --build

# EC2 workers (cuando el núcleo esté healthy)
docker compose -f docker-compose.workers.yaml up -d --build
```

Comprueba:
- Airflow `http://<IP pública núcleo>:8080`
- Flower `http://<IP pública núcleo>:5555` → deben salir `celery@worker-bronze`, `-silver`, `-gold`.

## 7. Actualizar DAGs

Los DAGs se leen del repo en **las dos** máquinas:

```bash
git pull   # en ambas EC2; los contenedores ven los cambios sin reiniciar
```

Si cambia `requirements.txt` o el `Dockerfile`, añade `up -d --build`.

## En tu PC

`docker compose up -d` (el `docker-compose.yaml` de siempre) sigue levantando todo en una sola
máquina para probar.
