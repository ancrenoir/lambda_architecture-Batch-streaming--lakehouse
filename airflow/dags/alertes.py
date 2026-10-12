import json
import logging
from urllib.error import URLError
from urllib.request import Request, urlopen

from airflow.models import Variable


logger = logging.getLogger(__name__)


def alerter_echec(context):
    task_instance = context["task_instance"]
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    run_id = task_instance.run_id

    logger.error(
        "airflow_task_failed dag_id=%s task_id=%s run_id=%s",
        dag_id,
        task_id,
        run_id,
    )

    webhook_url = Variable.get("ALERT_WEBHOOK_URL", default_var=None)

    if not webhook_url:
        logger.warning(
            "alert_webhook_not_configured dag_id=%s task_id=%s",
            dag_id,
            task_id,
        )
        return

    payload = {
        "text": (
            "WABA Airflow failure: "
            f"dag={dag_id}, task={task_id}, run={run_id}"
        ),
    }
    request = Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(request, timeout=10) as response:
            logger.info(
                "failure_alert_sent dag_id=%s task_id=%s status=%s",
                dag_id,
                task_id,
                response.status,
            )
    except (TimeoutError, URLError) as error:
        logger.error(
            "failure_alert_not_sent dag_id=%s task_id=%s error=%s",
            dag_id,
            task_id,
            error,
        )
