from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path

import clickhouse_connect
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPARK_JOB = PROJECT_ROOT / "spark_jobs" / "build_daily_stats.py"
log = logging.getLogger(__name__)

# Для планового запуска обе даты равны началу интервала — это предыдущий день.
# При ручном запуске их можно передать через параметры DAG.
FIRST_DATE = "{{ params.first_date or data_interval_start | ds }}"
LAST_DATE = "{{ params.last_date or params.first_date or data_interval_start | ds }}"


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get("CLICKHOUSE_DATABASE", "nyc_taxi"),
    )


with DAG(
    dag_id="ny_taxi_daily_stats",
    description="Дневная статистика поездок NYC Taxi: расчёт в Spark, хранение в ClickHouse",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
    catchup=False,
    params={"first_date": "", "last_date": ""},
    tags=["учебный", "spark", "clickhouse"],
) as dag:
    build_stats = SparkSubmitOperator(
        task_id="build_daily_stats_with_spark",
        application=str(SPARK_JOB),
        application_args=[
            "--first_date",
            FIRST_DATE,
            "--last_date",
            LAST_DATE,
        ],
        conn_id="spark_default",
        jars=os.environ["CLICKHOUSE_JDBC_JAR"],
        driver_memory="1g",
        executor_memory="1g",
        conf={
            "spark.sql.shuffle.partitions": "4",
            "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        },
        env_vars={
            "CLICKHOUSE_HOST": os.environ["CLICKHOUSE_HOST"],
            "CLICKHOUSE_PORT": os.environ.get("CLICKHOUSE_PORT", "8123"),
            "CLICKHOUSE_DATABASE": os.environ.get("CLICKHOUSE_DATABASE", "nyc_taxi"),
            "CLICKHOUSE_USER": os.environ["CLICKHOUSE_USER"],
            "CLICKHOUSE_PASSWORD": os.environ["CLICKHOUSE_PASSWORD"],
        },
    )

    @task
    def validate_result(first_date: str, last_date: str) -> dict[str, object]:
        try:
            period_start = date.fromisoformat(first_date)
            period_end = date.fromisoformat(last_date)
        except ValueError as error:
            raise ValueError(
                "Параметры first_date и last_date должны быть в формате ГГГГ-ММ-ДД"
            ) from error

        if period_start > period_end:
            raise ValueError("first_date не может быть позже last_date")

        client = clickhouse_client()
        try:
            row = client.query(
                f"""
                SELECT
                    count() AS aggregate_rows,
                    sum(trip_count) AS valid_trips,
                    min(pickup_date) AS min_date,
                    max(pickup_date) AS max_date
                FROM nyc_taxi.daily_trip_stats
                WHERE pickup_date BETWEEN toDate('{first_date}') AND toDate('{last_date}')
                """
            ).first_row

            source_trips = client.query(
                f"""
                SELECT count()
                FROM nyc_taxi.trips_small
                WHERE trip_distance > 0
                  AND total_amount > 0
                  AND dropoff_datetime > pickup_datetime
                  AND toDate(pickup_datetime)
                      BETWEEN toDate('{first_date}') AND toDate('{last_date}')
                """
            ).first_row[0]
        finally:
            client.close()

        aggregate_rows, valid_trips, min_date, max_date = row
        if aggregate_rows == 0 or valid_trips == 0:
            raise ValueError("Spark завершил расчёт, но итоговая таблица осталась пустой")
        if valid_trips != source_trips:
            raise ValueError(
                f"Число поездок не совпало: в источнике {source_trips}, в витрине {valid_trips}"
            )

        log.info(
            "Проверка пройдена: %s строк с агрегатами, %s поездок за период %s — %s",
            aggregate_rows,
            valid_trips,
            first_date,
            last_date,
        )

        return {
            "строк_с_агрегатами": aggregate_rows,
            "обработано_поездок": valid_trips,
            "first_date": first_date,
            "last_date": last_date,
            "первая_дата_с_данными": str(min_date),
            "последняя_дата_с_данными": str(max_date),
        }

    validation = validate_result(FIRST_DATE, LAST_DATE)
    build_stats >> validation
