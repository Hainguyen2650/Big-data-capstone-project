FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER root
RUN python -m playwright install-deps chromium
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers
RUN mkdir -p /opt/playwright-browsers && chown airflow:root /opt/playwright-browsers
USER airflow
RUN python -m playwright install chromium

COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts
COPY sql /opt/airflow/sql
