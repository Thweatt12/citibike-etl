FROM apache/airflow:3.0.6
ARG AIRFLOW_VERSION=3.0.6
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN python -m pip install --no-cache-dir "apache-airflow-providers-postgres" --constraint "${CONSTRAINT_URL}"
