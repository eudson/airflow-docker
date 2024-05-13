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
def dim_user_etl_dag():
    @task()
    def oracle_to_clickhouse_task(**context):
        context["ti"].xcom_push(key="exec_date_start", value=datetime.now().strftime('%Y-%m-%d %X'))
        source = 'engine_user'
        oracle_hook = OracleHook(oracle_conn_id='oracle_default')
        clickhouse_client = get_clickhouse_client()
        last_id = get_last_audit_info_record_last_id(clickhouse_client, source)

        where_clause = f'eu.ID > {last_id}' if last_id else '1 = 1'

        query = f"""
        SELECT
            eu.ID,
            ee.NAME,
            ee.NUIT,
            eer.ORGANIC_CLASSIFIER_ID,
            eu.CREATION_DATE,
            eu.ACCESS_LEVEL_ID,
            eu.ACCESS_PROFILE_ID,
            eu.UNIT_ID,
            eu.FISCAL_YEAR_ID
        FROM MEX.ENGINE_USER eu
            INNER JOIN MEX.EXTERNAL_ENTITY_ROLE eer ON eer.id = eu.PUBLIC_EMPLOYEE_ID
            INNER JOIN MEX.EXTERNAL_ENTITY ee ON ee.id = eer.EXTERNAL_ENTITY_ID
            WHERE {where_clause}
        ORDER BY eu.ID
        """

        result = oracle_hook.get_pandas_df(query)

        if not result.empty:
            result['ORGANIC_CLASSIFIER_ID'] = result['ORGANIC_CLASSIFIER_ID'].fillna(0).astype('int64')

            result['UNIT_ID'] = result['UNIT_ID'].fillna(0).astype('int64')

            push_audit_xcom_values(context=context, data=result, source=source)

            result = result.drop(columns=['CREATION_DATE'], axis=1)

            clickhouse_client.insert('dim_user', result.to_records(index=False),
                                     column_names=['engine_user_id', 'name', 'nuit', 'organic_classifier_id',
                                                   'access_level_id',
                                                   'access_profile_id', 'unit_id', 'fiscal_year_id'])

            context["ti"].xcom_push(key="exec_date_end", value=datetime.now().strftime('%Y-%m-%d %X'))
        else:
            logging.info(f'No new data found for {source}')

    audit_task = SourceDataAuditOperator(task_id='dim_user_etl_audit_task',
                                         source_dag_id='dim_user_etl_dag')

    oracle_to_clickhouse_task() >> audit_task


dim_user_etl_dag()
