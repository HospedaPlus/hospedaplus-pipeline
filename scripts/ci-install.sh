#!/usr/bin/env bash
# Installs Airflow + project requirements for CI the same way the Docker image does:
# Airflow pinned by its official constraints; our own pins in requirements.txt win.
set -euo pipefail
AIRFLOW_VERSION=3.1.5
PY=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
curl -fsSL "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt" \
  -o /tmp/constraints.txt
# Drop from the constraints every package we pin ourselves (and their tight deps).
names=$(grep -vE '^\s*(#|$)' requirements.txt | sed -E 's/[=<>!~ ].*//' | paste -sd'|')
grep -viE "^(${names}|botocore|s3transfer|py4j)==" /tmp/constraints.txt > /tmp/constraints-ci.txt
uv pip install "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt -c /tmp/constraints-ci.txt "$@"
