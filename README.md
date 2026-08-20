# NYC Taxi: Airflow, Spark и ClickHouse

Небольшой учебный проект, в котором Airflow запускает Spark-джобы, а результат
расчёта сохраняется в ClickHouse.

Джобы описываются в YAML: каждая объявляет входные и выходные таблицы, а Airflow
собирает DAG и зависимости между задачами по их lineage.

```text
trips_small → Spark → daily_trip_stats
                 ↑
              Airflow
```

## Что внутри

- `spark_jobs/` — код расчётов на PySpark;
- `jobs/` — описание джоб, параметров и lineage;
- `pipelines/` — расписание и состав DAG;
- `dags/` — универсальный генератор DAG;
- `compose.yaml` — локальный Airflow с GitDagBundle.

Текущая джоба ежедневно в 03:00 пересчитывает данные за предыдущий день. Перед
записью она удаляет результат за расчётный период, поэтому одну дату можно
безопасно запускать повторно.

## Запуск

Проект ожидает запущенный ClickHouse в Docker-сети
`clickhouse-superset-lab_default` и исходную таблицу `nyc_taxi.trips_small`.

```bash
cp .env.example .env
docker compose up --build -d
```

Airflow будет доступен на [http://localhost:8080](http://localhost:8080).
Логин и пароль standalone-режима можно найти в логах:

```bash
docker compose logs airflow | grep -i password
```

## Ручной пересчёт

Период задаётся включительно параметрами `first_date` и `last_date`:

```bash
docker exec ny-taxi-airflow airflow dags trigger ny_taxi_daily_stats \
  --conf '{"first_date":"2015-09-01","last_date":"2015-09-07"}'
```

Тот же JSON можно передать при ручном запуске DAG через интерфейс Airflow.

## Как добавить новую витрину

1. Добавить Spark-джобу в `spark_jobs/`.
2. Описать её `inputs` и `outputs` в новом файле внутри `jobs/`.
3. Добавить `job_id` в нужный файл из `pipelines/`.

Если вход одной джобы совпадёт с выходом другой, генератор автоматически создаст
между ними зависимость.
