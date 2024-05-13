from datetime import datetime, timedelta
import logging
from airflow.decorators import dag, task
import clickhouse_connect
from common.audit import SourceDataAuditOperator

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


@dag(
    start_date=datetime(2024, 1, 21),
    schedule=None,
    catchup=False,
    tags=['Testing'],
    default_args=default_args
)
def test_data_push_and_pull():
    @task
    def push_data_task(**context):
        try:
            client = clickhouse_connect.get_client(host='host.docker.internal', port=55002)
            logging.info('Connected successfully to clickhouse')
        except:
            logging.error('Error trying to connect to clickhouse')

        context["ti"].xcom_push(key="source", value="MySource")
        context["ti"].xcom_push(key="record_uuid", value="uuid1")
        context["ti"].xcom_push(key="record_date_created", value=datetime.now())
        context["ti"].xcom_push(key="start_id", value=467271)
        context["ti"].xcom_push(key="last_id", value=5747371)
        context["ti"].xcom_push(key="last_row_num", value=0)
        context["ti"].xcom_push(key="exec_date_start", value=datetime.now())
        context["ti"].xcom_push(key="exec_date_end", value=datetime.now())
        context["ti"].xcom_push(key="total_records", value=78980)

        return ""

    audit_task = SourceDataAuditOperator(task_id='my_audit_task', source_dag_id='test_data_push_and_pull')

    push_data_task() >> audit_task


test_data_push_and_pull()
