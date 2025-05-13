from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "ghazi",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="test_dag",
    default_args=default_args,
    description="🔧 Simple test DAG",
    schedule="@daily",
    catchup=False,
    tags=["test"],
) as dag:

    start = EmptyOperator(task_id="start")

    print_date = BashOperator(
        task_id="print_date",
        bash_command="echo The date is $(date)",
    )

    sleep = BashOperator(
        task_id="sleep",
        bash_command="sleep 5",
    )

    echo = BashOperator(
        task_id="echo_hello",
        bash_command='echo "Hello from test_dag!"',
    )

    end = EmptyOperator(task_id="end")

    start >> print_date >> sleep >> echo >> end
