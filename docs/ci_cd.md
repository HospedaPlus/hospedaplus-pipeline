# CI/CD II — Data Engineering

Workflow: `.github/workflows/ci-cd.yml` (GitHub Actions).

| Cuándo | Qué corre |
|---|---|
| Cada PR a `main` | `ruff` (lint), validación de los 3 docker compose, **validación de DAGs** (todos cargan, sin errores de import, cola y tag correctos, sin catchup), **pruebas de jobs Spark** con datos de ejemplo, **pruebas de calidad de datos**, build de la imagen |
| Merge a `main` | Lo mismo + **publica la imagen** en GHCR (`ghcr.io/hospedaplus/hospedaplus-pipeline:<sha>` y `:main`) + despliegue |
| Tag `vX.Y.Z` | Lo mismo + imagen con la versión (`:X.Y.Z`) → versionamiento de artefactos |

## Calidad de datos en el pipeline

`spark_jobs/quality.py` tiene los controles (no vacío, sin nulos, llave única, valores > 0, sin días faltantes).
Cada job silver los corre **antes de escribir**: si uno falla, la tarea de Airflow falla y el silver anterior queda intacto.
Hoy los usa `silver_fx` (tasa de cambio); los siguientes jobs silver los usarán igual.

## Correr las pruebas en local

```bash
uv venv -p 3.12 && source .venv/bin/activate
scripts/ci-install.sh pytest==8.4.2
AIRFLOW_HOME=$PWD/.airflow-ci python -m pytest -v
```

## Despliegue automático (APAGADO por defecto)

El job `deploy` revisa si existen los secretos; si falta alguno, **no hace nada** y deja un aviso.
Cuando existen, entra por SSH al núcleo y luego a workers y hace `git pull` + `docker compose up -d --build`
(y reinicia los workers para que se reconecten a RabbitMQ). Si las pruebas fallan, no despliega.

### Secretos (Settings → Secrets and variables → Actions → New repository secret)

| Secreto | Valor |
|---|---|
| `DEPLOY_SSH_KEY` | Contenido completo del `.pem` con el que entras por Termius (desde `-----BEGIN` hasta `-----END ...-----`) |
| `NUCLEO_HOST` | IP pública (o DNS público) de la EC2 núcleo |
| `WORKERS_HOST` | IP pública (o DNS público) de la EC2 workers |
| `DEPLOY_SSH_USER` | Opcional; por defecto `ubuntu` |

### Antes de activarlo: puerto 22

GitHub Actions entra desde IPs que cambian, y hoy `sg-nucleo` y `sg-workers` solo dejan el 22 desde tu IP.
Para que el deploy funcione hay que abrir el 22 (solo llave, sin contraseña) a `0.0.0.0/0` en ambos grupos.
Si prefieres no abrirlo, deja el despliegue apagado: el CI (pruebas + imagen) funciona igual sin esto.

Ojo: las IP públicas cambian si detienes y vuelves a iniciar la EC2; usa una Elastic IP o actualiza el secreto.
