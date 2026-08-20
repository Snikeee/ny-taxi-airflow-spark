from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import pendulum
import yaml
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, Asset


PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIRECTORY = PROJECT_ROOT / "jobs"
PIPELINES_DIRECTORY = PROJECT_ROOT / "pipelines"


def read_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        content = yaml.safe_load(stream)

    if not isinstance(content, dict):
        raise ValueError(f"Файл {path.name} должен содержать YAML-объект")
    return content


def required_string(config: dict[str, Any], key: str, source: Path) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"В файле {source.name} не заполнено поле {key}")
    return value


def string_list(config: dict[str, Any], key: str, source: Path) -> list[str]:
    value = config.get(key, [])
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(f"Поле {key} в файле {source.name} должно быть списком строк")
    return value


def load_jobs() -> dict[str, dict[str, Any]]:
    jobs: dict[str, dict[str, Any]] = {}

    for path in sorted(JOBS_DIRECTORY.glob("*.yaml")):
        config = read_yaml(path)
        job_id = required_string(config, "job_id", path)

        if job_id in jobs:
            raise ValueError(f"Джоба {job_id} описана больше одного раза")

        config["_source"] = path
        jobs[job_id] = config

    if not jobs:
        raise ValueError(f"В каталоге {JOBS_DIRECTORY} не найдено ни одной джобы")

    return jobs


def resolve_application(job: dict[str, Any]) -> Path:
    source = job["_source"]
    relative_path = required_string(job, "application", source)
    application = (PROJECT_ROOT / relative_path).resolve()

    try:
        application.relative_to(PROJECT_ROOT.resolve())
    except ValueError as error:
        raise ValueError(
            f"Путь application джобы {job['job_id']} выходит за пределы репозитория"
        ) from error

    if not application.is_file():
        raise ValueError(f"Не найден файл Spark-джобы: {application}")

    return application


def check_lineage(jobs: list[dict[str, Any]]) -> dict[str, set[str]]:
    producer_by_asset: dict[str, str] = {}
    dependencies: dict[str, set[str]] = defaultdict(set)

    for job in jobs:
        source = job["_source"]
        for output in string_list(job, "outputs", source):
            previous_producer = producer_by_asset.get(output)
            if previous_producer:
                raise ValueError(
                    f"Asset {output} создают сразу две джобы: "
                    f"{previous_producer} и {job['job_id']}"
                )
            producer_by_asset[output] = job["job_id"]

    for job in jobs:
        source = job["_source"]
        for input_asset in string_list(job, "inputs", source):
            producer = producer_by_asset.get(input_asset)
            if producer and producer != job["job_id"]:
                dependencies[job["job_id"]].add(producer)

    unresolved = {job["job_id"] for job in jobs}
    resolved: set[str] = set()
    while unresolved:
        ready = {
            job_id
            for job_id in unresolved
            if dependencies[job_id].issubset(resolved)
        }
        if not ready:
            cycle = ", ".join(sorted(unresolved))
            raise ValueError(f"В lineage найден цикл между джобами: {cycle}")
        resolved.update(ready)
        unresolved.difference_update(ready)

    return dependencies


def spark_environment(variable_names: list[str]) -> dict[str, str]:
    missing = [name for name in variable_names if not os.environ.get(name)]
    if missing:
        raise ValueError(
            "Не заданы переменные окружения для Spark: " + ", ".join(sorted(missing))
        )
    return {name: os.environ[name] for name in variable_names}


def build_dag(
    pipeline: dict[str, Any],
    source: Path,
    job_catalog: dict[str, dict[str, Any]],
) -> DAG:
    dag_id = required_string(pipeline, "dag_id", source)
    job_ids = string_list(pipeline, "jobs", source)
    if not job_ids:
        raise ValueError(f"В пайплайне {dag_id} нет джоб")

    unknown_jobs = sorted(set(job_ids) - set(job_catalog))
    if unknown_jobs:
        raise ValueError(
            f"В пайплайне {dag_id} указаны неизвестные джобы: {', '.join(unknown_jobs)}"
        )
    if len(job_ids) != len(set(job_ids)):
        raise ValueError(f"В пайплайне {dag_id} одна и та же джоба указана несколько раз")

    jobs = [job_catalog[job_id] for job_id in sorted(job_ids)]
    dependencies = check_lineage(jobs)

    spark_config = pipeline.get("spark", {})
    if not isinstance(spark_config, dict):
        raise ValueError(f"Поле spark в файле {source.name} должно быть YAML-объектом")

    env_names = string_list(spark_config, "env_vars", source)
    jars_env = required_string(spark_config, "jars_env", source)
    if not os.environ.get(jars_env):
        raise ValueError(f"Не задана переменная окружения {jars_env}")

    start_date = pendulum.parse(required_string(pipeline, "start_date", source))

    with DAG(
        dag_id=dag_id,
        description=pipeline.get("description"),
        schedule=required_string(pipeline, "schedule", source),
        start_date=start_date,
        catchup=bool(pipeline.get("catchup", False)),
        params=pipeline.get("params", {}),
        tags=string_list(pipeline, "tags", source),
    ) as dag:
        tasks: dict[str, SparkSubmitOperator] = {}

        for job in jobs:
            job_source = job["_source"]
            inputs = [Asset(uri) for uri in string_list(job, "inputs", job_source)]
            outputs = [Asset(uri) for uri in string_list(job, "outputs", job_source)]

            tasks[job["job_id"]] = SparkSubmitOperator(
                task_id=job["job_id"],
                application=str(resolve_application(job)),
                application_args=string_list(job, "arguments", job_source),
                conn_id=spark_config.get("conn_id", "spark_default"),
                jars=os.environ[jars_env],
                driver_memory=spark_config.get("driver_memory", "1g"),
                executor_memory=spark_config.get("executor_memory", "1g"),
                conf=spark_config.get("conf", {}),
                env_vars=spark_environment(env_names),
                inlets=inputs,
                outlets=outputs,
                doc=job.get("description"),
            )

        for consumer, producers in sorted(dependencies.items()):
            for producer in sorted(producers):
                tasks[producer] >> tasks[consumer]

    return dag


JOB_CATALOG = load_jobs()

for pipeline_path in sorted(PIPELINES_DIRECTORY.glob("*.yaml")):
    pipeline_config = read_yaml(pipeline_path)
    generated_dag = build_dag(pipeline_config, pipeline_path, JOB_CATALOG)
    globals()[generated_dag.dag_id] = generated_dag
