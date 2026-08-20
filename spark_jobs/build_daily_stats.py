from __future__ import annotations

import argparse
import os
from datetime import date

import clickhouse_connect
from pyspark.sql import SparkSession, functions as F


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Не задана обязательная переменная окружения: {name}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Считает дневные агрегаты NYC Taxi за указанный период"
    )
    parser.add_argument("--first_date", required=True, help="Начало периода в формате ГГГГ-ММ-ДД")
    parser.add_argument("--last_date", required=True, help="Конец периода в формате ГГГГ-ММ-ДД")
    args = parser.parse_args()

    try:
        args.first_date = date.fromisoformat(args.first_date)
        args.last_date = date.fromisoformat(args.last_date)
    except ValueError as error:
        raise ValueError("Параметры first_date и last_date должны быть в формате ГГГГ-ММ-ДД") from error

    if args.first_date > args.last_date:
        raise ValueError("first_date не может быть позже last_date")

    return args


def main() -> None:
    args = parse_args()
    first_date = args.first_date.isoformat()
    last_date = args.last_date.isoformat()

    host = required_env("CLICKHOUSE_HOST")
    port = os.environ.get("CLICKHOUSE_PORT", "8123")
    database = os.environ.get("CLICKHOUSE_DATABASE", "nyc_taxi")
    user = required_env("CLICKHOUSE_USER")
    password = required_env("CLICKHOUSE_PASSWORD")

    print(f"Начинаем пересчёт за период {first_date} — {last_date}", flush=True)

    client = clickhouse_connect.get_client(
        host=host,
        port=int(port),
        username=user,
        password=password,
        database=database,
    )
    try:
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
            PARTITION BY toYYYYMM(pickup_date)
            ORDER BY (pickup_date, pickup_ntaname)
            """
        )
        client.command(
            f"""
            ALTER TABLE nyc_taxi.daily_trip_stats
            DELETE WHERE pickup_date BETWEEN toDate('{first_date}') AND toDate('{last_date}')
            """,
            settings={"mutations_sync": 2},
        )
    finally:
        client.close()

    print("Старые агрегаты за расчётный период удалены", flush=True)

    jdbc_url = f"jdbc:clickhouse://{host}:{port}/{database}"
    jdbc_options = {
        "url": jdbc_url,
        "user": user,
        "password": password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }

    spark = (
        SparkSession.builder.appName("ny-taxi-daily-stats")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("Читаем и фильтруем поездки из ClickHouse…", flush=True)

    source_query = f"""
        (
            SELECT
                trip_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                fare_amount,
                tip_amount,
                total_amount,
                pickup_ntaname
            FROM nyc_taxi.trips_small
            WHERE trip_distance > 0
              AND total_amount > 0
              AND dropoff_datetime > pickup_datetime
              AND toDate(pickup_datetime)
                  BETWEEN toDate('{first_date}') AND toDate('{last_date}')
        ) AS valid_trips
    """

    trips = (
        spark.read.format("jdbc")
        .options(**jdbc_options)
        .option("dbtable", source_query)
        .option("fetchsize", "10000")
        .load()
    )

    stats = (
        trips.withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn(
            "duration_minutes",
            (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long"))
            / F.lit(60.0),
        )
        .groupBy("pickup_date", "pickup_ntaname")
        .agg(
            F.count("*").cast("long").alias("trip_count"),
            F.sum("passenger_count").cast("long").alias("passenger_count"),
            F.avg("trip_distance").cast("double").alias("avg_distance"),
            F.avg("fare_amount").cast("double").alias("avg_fare_amount"),
            F.avg("tip_amount").cast("double").alias("avg_tip_amount"),
            F.avg("total_amount").cast("double").alias("avg_total_amount"),
            F.avg("duration_minutes").cast("double").alias("avg_duration_minutes"),
        )
        .withColumn("processed_at", F.current_timestamp())
        .cache()
    )

    aggregate_rows = stats.count()
    if aggregate_rows == 0:
        stats.unpersist()
        spark.stop()
        raise RuntimeError(
            f"За период {first_date} — {last_date} не найдено поездок для расчёта"
        )

    valid_trips = stats.agg(F.sum("trip_count")).first()[0]
    print(f"Расчёт закончен: получилось {aggregate_rows} строк с агрегатами", flush=True)
    print(f"В расчёт вошло поездок: {valid_trips}", flush=True)
    print("Записываем результат в nyc_taxi.daily_trip_stats…", flush=True)

    (
        stats.repartition(4, "pickup_date")
        .write.format("jdbc")
        .options(**jdbc_options)
        .option("dbtable", "nyc_taxi.daily_trip_stats")
        .option("batchsize", "10000")
        .mode("append")
        .save()
    )

    client = clickhouse_connect.get_client(
        host=host,
        port=int(port),
        username=user,
        password=password,
        database=database,
    )
    try:
        result = client.query(
            f"""
            SELECT count(), sum(trip_count)
            FROM nyc_taxi.daily_trip_stats
            WHERE pickup_date BETWEEN toDate('{first_date}') AND toDate('{last_date}')
            """
        ).first_row
    finally:
        client.close()

    written_rows, written_trips = result
    if written_rows != aggregate_rows or written_trips != valid_trips:
        raise RuntimeError(
            "Проверка результата не пройдена: "
            f"рассчитано {aggregate_rows} строк и {valid_trips} поездок, "
            f"а записано {written_rows} строк и {written_trips} поездок"
        )

    stats.unpersist()
    print(
        f"Проверка пройдена: записано {written_rows} строк и {written_trips} поездок",
        flush=True,
    )
    print("Готово: данные записаны в ClickHouse", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
