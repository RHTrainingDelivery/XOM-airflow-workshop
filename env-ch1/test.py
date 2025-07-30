
import textwrap
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow.sdk import DAG
with DAG(
    "testing",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    description="Testing DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["testing"],
) as dag:

    t1 = BashOperator(
        task_id="print_string",
        bash_command="echo 'Hello world'",
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 10",
        retries=3,
    )
    
    t1 >> t2