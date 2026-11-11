import logging
import re
import subprocess

from airflow.timetables.trigger import CronTriggerTimetable
from airflow.operators.python import ShortCircuitOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import (
    PythonOperator,
    get_current_context,
)
from airflow.utils.types import DagRunType


PROJECT = "/opt/airflow/project"
PYTHON = "/opt/nyc-venv/bin/python"
DBT = "/opt/nyc-venv/bin/dbt"

logger = logging.getLogger(__name__)


def two_months_before(dt):
    month_index = dt.year * 12 + dt.month - 1 - 2
    year, month_zero = divmod(month_index, 12)
    return f"{year:04d}-{month_zero + 1:02d}"



def resolve_period():
    context = get_current_context()

    period = context["ti"].xcom_pull(
        task_ids="prepare_period"
    )

    if not period:
        raise ValueError("No processing period selected")

    return (
        period["start_month"],
        period["end_month"],
    )



def run_command(command):
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as process:

        for line in process.stdout:
            logger.info("%s", line.rstrip())

        return_code = process.wait()

    if return_code != 0:
        raise subprocess.CalledProcessError(
            return_code, command
        )



def run_download_taxi():
    start, end = resolve_period()

    try:
        run_command([
            PYTHON, "-u",
            f"{PROJECT}/scripts/download_taxi.py",
            "--start", start,
            "--end", end,
        ])

    except subprocess.CalledProcessError as exc:
        if exc.returncode == 75:
            raise AirflowSkipException(
                "TLC data not published yet. "
                "Will check again on the next scheduled run."
            ) from exc

        raise



def run_taxi_batch():
    start, end = resolve_period()

    run_command([
        PYTHON, "-u",
        f"{PROJECT}/scripts/ingest_taxi_batch.py",
        "--start", start,
        "--end", end,
    ])


def run_weather_range():
    start, end = resolve_period()
    conf = get_current_context()["dag_run"].conf or {}

    simulate = conf.get("simulate_weather_failure", False)

    if not isinstance(simulate, bool):
        raise ValueError(
            "simulate_weather_failure must be boolean"
        )

    command = [
        PYTHON, "-u",
        f"{PROJECT}/scripts/refresh_weather_range.py",
        "--start", start,
        "--end", end,
    ]

    if simulate:
        command.append("--simulate-failure")

    run_command(command)


def prepare_period():
    context = get_current_context()
    conf = context["dag_run"].conf or {}

    latest = two_months_before(
        context["data_interval_end"]
    )

    has_start = "start_month" in conf
    has_end = "end_month" in conf

    if has_start != has_end:
        raise ValueError(
            "Both start_month and end_month are required"
        )

    if has_start:
        start = conf["start_month"]
        end = conf["end_month"]

    else:
        result = subprocess.run(
            [
                PYTHON,
                f"{PROJECT}/scripts/select_backfill_month.py",
                "--through",
                latest,
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        selected = result.stdout.strip()

        if selected == "NONE":
            logger.info("No missing months found")
            return None

        start = selected
        end = selected

    pattern = r"\d{4}-(0[1-9]|1[0-2])"

    if (
        not isinstance(start, str)
        or not isinstance(end, str)
        or not re.fullmatch(pattern, start)
        or not re.fullmatch(pattern, end)
        or start < "2025-01"
        or start > end
        or end > latest
    ):
        raise ValueError("Invalid processing period")

    logger.info(
        "Prepared processing period: %s to %s",
        start, end
    )

    return {
        "start_month": start,
        "end_month": end,
    }


def has_work():
    context = get_current_context()

    period = context["ti"].xcom_pull(
        task_ids="prepare_period"
    )

    return period is not None


def run_validate_weather():
    start, end = resolve_period()

    run_command([
        PYTHON,
        "-u",
        f"{PROJECT}/scripts/validate_weather_range.py",
        "--start", start,
        "--end", end,
    ])


with DAG(
    dag_id="nyc_taxi_pipeline",
    description="NYC Taxi and Weather Data Pipeline",
    start_date=datetime(
        2026, 11, 11, tzinfo=timezone.utc
    ),
    
    schedule=CronTriggerTimetable(
        "0 7 * * *",
        timezone="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["nyc", "data-engineering"],
) as dag:

    
    validate_weather = PythonOperator(
        task_id="validate_weather",
        python_callable=run_validate_weather,
    )


    prepare_period_task = PythonOperator(
        task_id="prepare_period",
        python_callable=prepare_period,
    )

    has_work_task = ShortCircuitOperator(
        task_id="has_work",
        python_callable=has_work,
    )


    download_taxi = PythonOperator(
        task_id="download_taxi",
        python_callable=run_download_taxi,
    )

    ingest_taxi_batch = PythonOperator(
        task_id="ingest_taxi_batch",
        python_callable=run_taxi_batch,
    )

    ingest_zones = BashOperator(
        task_id="ingest_zones",
        bash_command=(
            f"{PYTHON} {PROJECT}/scripts/ingest_zones.py"
        ),
    )

    refresh_weather = PythonOperator(
        task_id="refresh_weather",
        python_callable=run_weather_range,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT}/dbt && "
            f"{DBT} build --profiles-dir ."
        ),
    )

    export_metabase = BashOperator(
        task_id="export_metabase",
        bash_command=(
            f"{PYTHON} "
            f"{PROJECT}/scripts/export_metabase.py"
        ),
    )

    
    
    (
        prepare_period_task
        >> has_work_task
        >> download_taxi
        >> ingest_taxi_batch
        >> ingest_zones
        >> refresh_weather
        >> validate_weather
        >> dbt_build
        >> export_metabase
    )


