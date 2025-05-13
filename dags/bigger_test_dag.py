from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ─── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "ghazi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

# ─── Branching function ────────────────────────────────────────────────────────
def choose_path(count) -> str:
    """Branch based on record count (casts to int)."""
    count = int(count)
    return "load.load_success" if count > 0 else "load.load_empty"

# ─── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="bigger_test_dag",
    default_args=default_args,
    description="🔧 Bigger test DAG: ETL + branching + TaskGroup",
    schedule="@hourly",
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["test", "etl"],
) as dag:

    # ─── Start ─────────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ─── Extract group ─────────────────────────────────────────────────────────
    with TaskGroup("extract", tooltip="Download & inspect data") as extract:
        download = BashOperator(
            task_id="download_data",
            bash_command="curl -sSL https://httpbin.org/uuid > /tmp/uuid.json"
        )
        inspect = BashOperator(
            task_id="inspect_data",
            bash_command="jq . /tmp/uuid.json | tee /tmp/inspect.log"
        )

    # ─── Transform (TaskFlow) ───────────────────────────────────────────────────
    @task
    def transform() -> int:
        import json
        data = json.loads(open("/tmp/uuid.json").read())
        count = 1 if "uuid" in data else 0
        return count  # returned via XCom

    transform_count = transform()

    # ─── Branch ─────────────────────────────────────────────────────────────────
    branch = BranchPythonOperator(
        task_id="branch_on_count",
        python_callable=choose_path,
        op_args=["{{ ti.xcom_pull(task_ids='transform') }}"],
    )

    # ─── Load group ────────────────────────────────────────────────────────────
    with TaskGroup("load", tooltip="Load to target system") as load:
        load_success = BashOperator(
            task_id="load_success",
            bash_command='echo "Loaded {{ ti.xcom_pull(task_ids=\'transform\') }} records"',
        )
        load_empty = BashOperator(
            task_id="load_empty",
            bash_command='echo "No records to load, skipping"',
        )

    # ─── End ───────────────────────────────────────────────────────────────────
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # ─── Set dependencies ───────────────────────────────────────────────────────
    start >> extract >> transform_count >> branch
    branch >> load_success >> end
    branch >> load_empty >> end
