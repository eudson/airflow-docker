from airflow.models import BaseOperator
import logging

from common.utils import get_clickhouse_client

last_record_query = "SELECT * FROM etl_source_data_audit WHERE source = %(source)s ORDER BY exec_date_end DESC LIMIT 1"


def push_audit_xcom_values(context, data, source, keys=None, processed_records=None,
                           failed_records=None):
    id_key = 'ID'
    date_key = 'CREATION_DATE'

    if keys:
        id_key = keys['id_key']
        date_key = keys['date_key']

    last_record = data.tail(1)
    start_id = data.head(1)[id_key].values[0]
    last_id = last_record[id_key].values[0]
    last_record_creation_date = data.at[data.index[-1], date_key]
    total_records = len(data)
    total_processed_records = total_records if processed_records is None else int(processed_records)
    context["ti"].xcom_push(key="source", value=source)
    context["ti"].xcom_push(key="last_record_creation_date", value=last_record_creation_date.strftime('%Y-%m-%d %X'))
    context["ti"].xcom_push(key="start_id", value=start_id)
    context["ti"].xcom_push(key="last_id", value=last_id)
    context["ti"].xcom_push(key="last_row_num", value=0)
    context["ti"].xcom_push(key="total_records", value=total_records)
    context["ti"].xcom_push(key="processed_records", value=total_processed_records)
    context["ti"].xcom_push(key="failed_records", value=0 if failed_records is None else int(failed_records))


def get_last_audit_info_record(clickhouse_client, source):
    return clickhouse_client.query_df(last_record_query, parameters={'source': source})


def get_last_audit_info_record_last_id(clickhouse_client, source):
    audit_last_record = get_last_audit_info_record(clickhouse_client, source)
    if not audit_last_record.empty:
        return int(audit_last_record['last_id'].iloc[0])
    return None


class SourceDataAuditOperator(BaseOperator):

    def __init__(self, source_dag_id: str, **kwargs):
        super().__init__(**kwargs)
        self.source_dag_id = source_dag_id

    def execute(self, context):
        source = context["ti"].xcom_pull(key="source", dag_id=self.source_dag_id)

        last_record_creation_date = context["ti"].xcom_pull(key="last_record_creation_date", dag_id=self.source_dag_id)
        start_id = context["ti"].xcom_pull(key="start_id", dag_id=self.source_dag_id)
        last_id = context["ti"].xcom_pull(key="last_id", dag_id=self.source_dag_id)
        last_row_num = context["ti"].xcom_pull(key="last_row_num", dag_id=self.source_dag_id)
        exec_date_start = context["ti"].xcom_pull(key="exec_date_start", dag_id=self.source_dag_id)
        exec_date_end = context["ti"].xcom_pull(key="exec_date_end", dag_id=self.source_dag_id)
        total_records = context["ti"].xcom_pull(key="total_records", dag_id=self.source_dag_id)
        processed_records = context["ti"].xcom_pull(key="processed_records", dag_id=self.source_dag_id)
        failed_records = context["ti"].xcom_pull(key="failed_records", dag_id=self.source_dag_id)

        if exec_date_end:
            client = get_clickhouse_client()

            logging.info(f'Writing audit data for {source}')

            insert_query = (f"INSERT INTO scda_analysis.etl_source_data_audit (source,last_record_creation_date, "
                            f"start_id, last_id, last_row_num,exec_date_start, exec_date_end, "
                            f"total_records, processed_records, failed_records) "
                            f"VALUES ('{source}', '{last_record_creation_date}', {start_id}, {last_id},"
                            f"{last_row_num}, '{exec_date_start}','{exec_date_end}',{total_records},"
                            f"{processed_records},{failed_records})")

            logging.info('Query to execute')
            logging.info(insert_query)
            client.command(insert_query)

            logging.info(f'Audit data inserted successfully for {source}')
        else:
            logging.info('No audit data to process')
