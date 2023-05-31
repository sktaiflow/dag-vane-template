"""
### Description

- apache-airflow-providers-sktvane 패키지 유효성 검사
"""
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from slack_plugin import get_fail_alert


def _test():
    raise AirflowException("AirflowException has occurred!")


with DAG(
    dag_id="dag_plugins_import",
    schedule_interval=None,
    default_args={
        "owner": "유상기",
        "start_date": datetime(2023, 5, 17),
        "on_failure_callback": get_fail_alert(email="lapetus@sk.com"),
    },
    catchup=False,
) as dag:
    PythonOperator(task_id="temp", python_callable=_test)

dag.doc_md = __doc__
