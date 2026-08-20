ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG CLICKHOUSE_JDBC_VERSION=0.9.8

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl default-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/airflow/jars \
    && curl --fail --location --retry 3 \
      "https://github.com/ClickHouse/clickhouse-java/releases/download/v${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" \
      --output "/opt/airflow/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" \
    && chown -R airflow:root /opt/airflow/jars

USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --requirement /tmp/requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
