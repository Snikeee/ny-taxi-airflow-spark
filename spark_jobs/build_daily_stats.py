from __future__ import annotations

import os

from pyspark.sql import SparkSession, functions as F


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Не задана обязательная переменная окружения: {name}")
    return value


def main() -> None:
    host = required_env("CLICKHOUSE_HOST")
    port = os.environ.get("CLICKHOUSE_PORT", "8123")
    database = os.environ.get("CLICKHOUSE_DATABASE", "nyc_taxi")
    user = required_env("CLICKHOUSE_USER")
    password = required_env("CLICKHOUSE_PASSWORD")

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

    print("Подключились к ClickHouse. Читаем и фильтруем поездки…", flush=True)

    source_query = """
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
    print(f"Расчёт закончен: получилось {aggregate_rows} строк с агрегатами", flush=True)
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

    stats.unpersist()
    print("Готово: данные записаны в ClickHouse", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
