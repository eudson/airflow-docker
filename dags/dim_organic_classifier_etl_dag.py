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
def dim_organic_classifier_etl_dag():
    @task()
    def oracle_to_clickhouse(**context):
        context["ti"].xcom_push(key="exec_date_start", value=datetime.now().strftime('%Y-%m-%d %X'))

        clickhouse_client = get_clickhouse_client()

        source = 'organic_classifier'

        oracle_hook = OracleHook(oracle_conn_id='oracle_default')

        last_id = get_last_audit_info_record_last_id(clickhouse_client, source)

        where_clause = f'oc.ID > {last_id}' if last_id else '1 = 1'

        query = f"""
            SELECT oc.ID,
                   oc.FISCAL_YEAR_ID,
                   oc.CREATION_DATE,
                   oc.CODE,
                   oc.DESCRIPTION                                                   AS ORGANIC_CLASSIFIER_DESCRIPTION,
                   CASE WHEN tc.DESCRIPTION IS NULL THEN ' ' ELSE tc.DESCRIPTION END AS TERRITORY_CLASSIFIER_DESCRIPTION
            FROM MEX.ORGANIC_CLASSIFIER oc
                     LEFT JOIN MEX.TERRITORY_CLASSIFIER tc ON tc.ID = oc.TERRITORY_CLASSIFIER_ID
            WHERE {where_clause}
            ORDER BY oc.ID ASC
        """

        result = oracle_hook.get_pandas_df(query)

        if not result.empty:
            logging.info(f'Migrating data for  {source}')
            push_audit_xcom_values(context=context, data=result, source=source)

            result = result.drop(columns=['CREATION_DATE'], axis=1)

            clickhouse_client.insert('dim_organic_classifier', result.to_records(index=False),
                                     column_names=['source_id', 'fiscal_year_id', 'code', 'description',
                                                   'territory_classifier_description'])

            context["ti"].xcom_push(key="exec_date_end", value=datetime.now().strftime('%Y-%m-%d %X'))
        else:
            logging.info(f'No new data found for {source}')

    audit_task = SourceDataAuditOperator(task_id='dim_organic_classifier_etl_audit_task',
                                         source_dag_id='dim_organic_classifier_etl_dag')

    oracle_to_clickhouse() >> audit_task


dim_organic_classifier_etl_dag()
