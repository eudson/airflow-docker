import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from common.audit import SourceDataAuditOperator, push_audit_xcom_values, get_last_audit_info_record_last_id
from airflow.providers.mongo.hooks.mongo import MongoHook
from common.utils import cast_to_int, format_date
import pandas as pd

from common.utils import get_clickhouse_client
from airflow.models import Variable


@dag(
    schedule='@daily',
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=["facts"],
)
def fact_user_transaction_log_etl_dag():
    @task()
    def extract_task(**context):
        context["ti"].xcom_push(key="exec_date_start", value=datetime.now().strftime('%Y-%m-%d %X'))
        clickhouse_client = get_clickhouse_client()
        source = 'user_transaction_log'
        query_limit = int(Variable.get('transaction.log.limit'))

        try:
            hook = MongoHook(mongo_conn_id='mongo_default')
            client = hook.get_conn()
            db = client.auditlog_test
            audit_entry_collection = db.AuditEntry_01
            query_definition = {}

            last_id = get_last_audit_info_record_last_id(clickhouse_client, source)

            if last_id:
                query_definition["id"] = {"$gt": last_id}
                logging.info(f'Query definition updated to: {query_definition}')

            fields_to_return = {"id": 1, "aplicationDode": 1, "month": 1, "year": 1, "creationDate": 1, "createdBy": 1,
                                "clientHostAddress": 1, "serverHostAddress": 1,
                                "payload.audit-data.audit-info.transactionCode": 1}


            results_after = (audit_entry_collection.find(query_definition, fields_to_return)
                             .sort({'id': 1}).limit(query_limit))

            records = list()
            error_records = list()
            results_list = list(results_after)

            size = len(results_list)

            if size == 0:
                logging.info('No new data found to be processed for user transaction log')
            else:
                logging.info(f'Found {size} records to be processed')

                for document in results_list:
                    try:
                        transaction_code = document['payload']['audit-data']['audit-info']['transactionCode']

                        data = {
                            'auditEntryId': document['id'],
                            'auditEntryCreationDate': document['creationDate'],
                            'serverHostAddress': document['serverHostAddress'],
                            'clientHostAddress': document['clientHostAddress'],
                            'transactionCode': transaction_code,
                            'applicationCode': document['aplicationDode'],
                            'createdBy': document['createdBy'],
                            'month': document['month'],
                            'year': document['year']
                        }

                        records.append(data)
                    except KeyError as error:
                        logging.info(f'Error, key {error} not found while processing document {document} ')
                        error_records.append({
                            'source': source,
                            'source_id': str(document['id']),
                            'error_message': f'KeyError {error}',
                            'record_payload': str(document),
                            'timestamp': datetime.now()
                        })

                errors_size = len(error_records)
                if errors_size > 0:
                    errors_df = pd.DataFrame(error_records)
                    errors_df['timestamp'] = pd.to_datetime(errors_df['timestamp'])
                    clickhouse_client.insert_df('etl_record_error_log', errors_df)

                df = pd.DataFrame(records)

                df['auditEntryCreationDate'] = pd.to_datetime(df['auditEntryCreationDate'])
                clickhouse_client.insert_df('temp_user_transaction_log', df)
                results_after = clickhouse_client.query_df('SELECT * FROM temp_user_transaction_log')
                logging.info(f'Found {len(results_after)} in temp table after insert')
                data_processed_count = len(df)
                logging.info('Extraction Summary')
                logging.info(f'Records found: {data_processed_count + errors_size}')
                logging.info(f'Records processed successfully: {data_processed_count}')
                logging.info(f'Records with errors: {errors_size}')

                df_audit = pd.DataFrame(results_list)

                push_audit_xcom_values(context=context, data=df_audit, source=source,
                                       keys={'id_key': 'id', 'date_key': 'creationDate'},
                                       processed_records=data_processed_count,
                                       failed_records=errors_size)

                context["ti"].xcom_push(key="exec_date_end", value=datetime.now().strftime('%Y-%m-%d %X'))
        except Exception as ex:
            logging.error(f'Error connecting to mongo: {ex}')
            raise ex

    @task
    def transform_and_load():
        clickhouse_client = get_clickhouse_client()

        results = clickhouse_client.query_df('SELECT * FROM temp_user_transaction_log')

        if not results.empty:
            logging.info(f'Found {len(results)} records on the temp table')
            insert_query = """
            INSERT INTO fact_user_transaction_log(log_creation_date, user_id, user_nuit, user_full_name, user_display,
                                      user_access_profile_code, user_access_profile_description,
                                      user_access_profile_display, user_organic_unit_code,
                                      user_organic_unit_description, user_organic_unit_display, transaction_code,
                                      transaction_description, transaction_display, application_code,
                                      application_name, application_display, fiscal_year, month, month_display,month_year,
                                      month_year_display, server_host_address, client_host_address, audit_entry_id)
            SELECT t.auditEntryCreationDate                          AS log_creation_date,
                   t.createdBy                                       AS user_id,
                   du.nuit                                           AS user_nuit,
                   du.name                                           AS user_full_name,
                   concat(du.nuit, ' - ', du.name)                   AS user_display,
                   ap.code                                           AS user_access_profile_code,
                   ap.name                                           AS user_access_profile_description,
                   concat(ap.code, ' - ', ap.name)                   AS user_access_profile_display,
                   doc.code                                          AS user_organic_unit_code,
                   doc.description                                   AS user_organic_unit_description,
                   concat(doc.code, ' - ', doc.description)          AS user_organic_unit_display,
                   t.transactionCode                                 AS transaction_code,
                   dtt.description                                   AS transaction_description,
                   concat(t.transactionCode, ' - ', dtt.description) AS transaction_display,
                   t.applicationCode                                 AS application_code,
                   da.description                                    AS application_name,
                   concat(t.applicationCode, ' - ', da.description)  AS application_display,
                   t.year                                            AS fiscal_year,
                   t.month                                           AS month,
                   get_month_name(month)                             AS month_display,
                   concat(t.month, ' - ', t.year)                    AS month_year,
                   concat(get_month_name(month), ' - ', t.year)      AS month_year_display,
                   t.serverHostAddress                               AS server_host_address,
                   t.clientHostAddress                               AS client_host_address,
                   t.auditEntryId                                    AS audit_entry_id
            FROM temp_user_transaction_log t
                     INNER JOIN dim_user du on du.engine_user_id = t.createdBy
                     INNER JOIN dim_access_profile ap ON ap.source_id == du.access_profile_id
                     INNER JOIN dim_application da on da.code = t.applicationCode
                     INNER JOIN dim_fiscal_year dfa on dfa.year = t.year
                -- leaving this as left join because not all users have an organic_classifier
                     LEFT JOIN dim_organic_classifier doc
                               on doc.source_id = du.organic_classifier_id
                     LEFT JOIN dim_transaction_type dtt on dtt.application_id = da.source_id and dtt.code = t.transactionCode and
                                                           dtt.fiscal_year_id = dfa.source_id;
            """

            logging.info('Loading data into fact_user_history table')
            clickhouse_client.command(insert_query)
            logging.info('Data successfully loaded')
            logging.info('Deleting temporary data from temp_user_updates_log')
            clickhouse_client.command("DELETE FROM temp_user_transaction_log WHERE 1 = 1")
            logging.info('Temporary data successfully deleted')
        else:
            logging.info('No data found on the temp table')

    audit_task = SourceDataAuditOperator(task_id='extract_audit_task',
                                         source_dag_id='fact_user_transaction_log_etl_dag')

    extract_task() >> audit_task ## >> transform_and_load()


fact_user_transaction_log_etl_dag()
