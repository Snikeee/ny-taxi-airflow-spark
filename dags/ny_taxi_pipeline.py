from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import clickhouse_connect
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPARK_JOB = PROJECT_ROOT / "spark_jobs" / "build_daily_stats.py"
log = logging.getLogger(__name__)


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
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["учебный", "spark", "clickhouse"],
) as dag:

    @task
    def prepare_target() -> None:
        log.info("Готовим таблицу nyc_taxi.daily_trip_stats к новому расчёту")
        client = clickhouse_client()
        client.command(
            """
            CREATE TABLE IF NOT EXISTS nyc_taxi.daily_trip_stats
            (
                pickup_date Date,
                pickup_ntaname LowCardinality(String),
                trip_count UInt64,
                passenger_count UInt64,
                avg_distance Float64,
                avg_fare_amount Float64,
                avg_tip_amount Float64,
                avg_total_amount Float64,
                avg_duration_minutes Float64,
                processed_at DateTime
            )
            ENGINE = MergeTree
            ORDER BY (pickup_date, pickup_ntaname)
            """
        )
        # Пока пересчитываем витрину целиком. Так повторный запуск не создаст дубликаты.
        client.command("TRUNCATE TABLE nyc_taxi.daily_trip_stats")
        log.info("Таблица готова: старые результаты удалены")

    build_stats = SparkSubmitOperator(
        task_id="build_daily_stats_with_spark",
        application=str(SPARK_JOB),
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
    def validate_result() -> dict[str, object]:
        client = clickhouse_client()
        row = client.query(
            """
            SELECT
                count() AS aggregate_rows,
                sum(trip_count) AS valid_trips,
                min(pickup_date) AS min_date,
                max(pickup_date) AS max_date
            FROM nyc_taxi.daily_trip_stats
            """
        ).first_row

        aggregate_rows, valid_trips, min_date, max_date = row
        if aggregate_rows == 0 or valid_trips == 0:
            raise ValueError("Spark завершил расчёт, но итоговая таблица осталась пустой")

        log.info(
            "Проверка пройдена: %s строк с агрегатами, %s поездок за период %s — %s",
            aggregate_rows,
            valid_trips,
            min_date,
            max_date,
        )

        return {
            "строк_с_агрегатами": aggregate_rows,
            "обработано_поездок": valid_trips,
            "начало_периода": str(min_date),
            "конец_периода": str(max_date),
        }

    prepare_target() >> build_stats >> validate_result()
