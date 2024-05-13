import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.oracle.hooks.oracle import OracleHook
from common.audit import SourceDataAuditOperator, push_audit_xcom_values, get_last_audit_info_record_last_id
from common.utils import get_clickhouse_client


@dag(
    schedule='@once',
    catchup=False,
    start_date=datetime.now(),
    tags=["dimensions"],
)
def dim_application_etl_dag():
    @task()
    def oracle_to_clickhouse_task(**context):
        context["ti"].xcom_push(key="exec_date_start", value=datetime.now().strftime('%Y-%m-%d %X'))

        clickhouse_client = get_clickhouse_client()

        oracle_hook = OracleHook(oracle_conn_id='oracle_default')

        source = 'application'

        last_id = get_last_audit_info_record_last_id(clickhouse_client, source)

        where_clause = f'ID > {last_id}' if last_id else '1 = 1'

        query = f"SELECT ID, CODE, DESCRIPTION, CREATION_DATE FROM MEX.APPLICATION WHERE {where_clause} ORDER BY ID"

        result = oracle_hook.get_pandas_df(query)

        if not result.empty:
            push_audit_xcom_values(context=context, data=result, source=source)

            result = result.drop(columns=['CREATION_DATE'], axis=1)

            clickhouse_client.insert('dim_application', result.to_records(index=False),
                                     column_names=['source_id', 'code', 'description'])

            context["ti"].xcom_push(key="exec_date_end", value=datetime.now().strftime('%Y-%m-%d %X'))
        else:
            logging.info(f'No new data found for {source}')

    audit_task = SourceDataAuditOperator(task_id='dim_application_etl_audit_task',
                                         source_dag_id='dim_application_etl_dag')

    oracle_to_clickhouse_task() >> audit_task


dim_application_etl_dag()
