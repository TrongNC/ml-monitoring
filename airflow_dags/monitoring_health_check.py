import os
from datetime import datetime

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def notify_telegram(context):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    task_instance = context["task_instance"]
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    run_id = context.get("run_id", "unknown")
    message = (
        "<b>Airflow task failed</b>\n"
        f"DAG: <code>{dag_id}</code>\n"
        f"Task: <code>{task_id}</code>\n"
        f"Run: <code>{run_id}</code>"
    )
    response = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        },
        timeout=10,
    )
    response.raise_for_status()


def check_service(service_name, url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") not in {"healthy", "ok"}:
        raise RuntimeError(f"{service_name} returned an unhealthy status: {payload}")


with DAG(
    dag_id="monitoring_health_check",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    on_failure_callback=notify_telegram,
    tags=["monitoring", "health-check"],
) as dag:
    check_api = PythonOperator(
        task_id="check_model_api",
        python_callable=check_service,
        op_kwargs={"service_name": "model-api", "url": "http://api:8000/health"},
    )

    check_evidently = PythonOperator(
        task_id="check_evidently",
        python_callable=check_service,
        op_kwargs={"service_name": "evidently", "url": "http://evidently:8001/health"},
    )

    [check_api, check_evidently]
