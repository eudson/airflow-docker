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
def fact_user_history_etl_dag():
    @task()
    def extract_task(**context):
        context["ti"].xcom_push(key="exec_date_start", value=datetime.now().strftime('%Y-%m-%d %X'))
        clickhouse_client = get_clickhouse_client()
        source = 'user_history'

        try:
            hook = MongoHook(mongo_conn_id='mongo_default')
            client = hook.get_conn()
            db = client.auditlog_test
            audit_entry_collection = db.AuditEntry_01
            query_definition = {
                "payload.audit-data.audit-info.transactionCode": "995"
            }

            last_id = get_last_audit_info_record_last_id(clickhouse_client, source)

            if last_id:
                query_definition["id"] = {"$gt": last_id}
                logging.info(f'Query definition updated to: {query_definition}')
            results_after = audit_entry_collection.find(query_definition).sort({'id': 1}).limit(
                int(Variable.get('transaction.995.limit')))

            records = list()
            error_records = list()
            results_list = list(results_after)

            size = len(results_list)

            if size == 0:
                logging.info('No new data found to be processed for transaction 995')
            else:
                logging.info(f'Found {size} records to be processed')
                for document in results_list:
                    try:
                        audit_data = document['payload']['audit-data']
                        service = audit_data['service']
                        try:
                            rdbms_record = service['audit-records']['rdbms-record']
                        except KeyError:
                            rdbms_record = service['audit-records']['service']['audit-records']['rdbms-record']
                        audit_info = audit_data['audit-info']

                        if isinstance(rdbms_record, list):
                            rdbms_record = rdbms_record[0]

                        # Parse date strings into ClickHouse-friendly format
                        update_date = format_date(rdbms_record['updateDate'])
                        creation_date = format_date(rdbms_record['creationDate'])
                        activation_date = format_date(rdbms_record['activationDate'])

                        # Prepare data for ClickHouse insertion

                        data = {
                            'auditEntryId': document['id'],
                            'auditEntryCreationDate': document['creationDate'],
                            'methodName': service['methodName'],
                            'methodCode': cast_to_int(service['code']),
                            'userDetailsId': cast_to_int(rdbms_record['userDetailsId']),
                            'accessProfileId': cast_to_int(rdbms_record['accessProfileId']),
                            'active': rdbms_record['active'],
                            'activatedBy': cast_to_int(rdbms_record['activatedBy']),
                            'fiscalYearId': cast_to_int(rdbms_record['fiscalYearId']),
                            'accessLevelId': cast_to_int(rdbms_record['accessLevelId']),
                            'createdBy': cast_to_int(rdbms_record['createdBy']),
                            'publicEmployeeId': cast_to_int(rdbms_record['publicEmployeeId']),
                            'userCreationRuleId': cast_to_int(rdbms_record['userCreationRuleId']),
                            'entityName': rdbms_record['entityName'],
                            'unitId': cast_to_int(rdbms_record['unitId']),
                            'state': cast_to_int(rdbms_record['state']),
                            'userId': cast_to_int(rdbms_record['id']),
                            'action': rdbms_record['action'],
                            'updatedBy': cast_to_int(rdbms_record['updatedBy']),
                            'updateDate': update_date,
                            'creationDate': creation_date,
                            'activationDate': activation_date,
                            'userPasswordId': cast_to_int(rdbms_record['userPasswordId']),
                            'serverHostAddress': audit_info['serverHostAddress'],
                            'clientHostAddress': audit_info['clientHostAddress'],
                            'sessionId': audit_info['sessionId'],
                            'transactionCode': cast_to_int(audit_info['transactionCode']),
                            'applicationCode': audit_info['applicationCode'],
                            'userName': audit_info['userName']
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

                df = pd.DataFrame(records)

                df['updateDate'] = pd.to_datetime(df['updateDate'])
                df['auditEntryCreationDate'] = pd.to_datetime(df['auditEntryCreationDate'])
                df['creationDate'] = pd.to_datetime(df['creationDate'])
                df['activationDate'] = pd.to_datetime(df['activationDate'])

                clickhouse_client.insert_df('temp_user_updates_log', df)
                results_after = clickhouse_client.query_df('SELECT * FROM temp_user_updates_log')
                logging.info(f'Found {len(results_after)} in temp table after insert')
                errors_size = len(error_records)
                if errors_size > 0:
                    errors_df = pd.DataFrame(error_records)
                    errors_df['timestamp'] = pd.to_datetime(errors_df['timestamp'])
                    clickhouse_client.insert_df('etl_record_error_log', errors_df)

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

        results = clickhouse_client.query_df('SELECT * FROM temp_user_updates_log')

        if not results.empty:
            logging.info(f'Found {len(results)} records on the temp table')
            insert_query = """
            INSERT INTO fact_user_history (fiscal_year, user_nuit, user_full_name, user_display_name, user_profile, user_unit_code,
                               user_unit_description, user_unit_display_name, creation_date,
                               update_date, active, created_by_nuit, created_by_full_name, created_by_profile, action,
                               log_creation_date, server_host_address, client_host_address, session_id, updated_by_nuit,
                               updated_by_full_name, updated_by_display_name,updated_by_profile,
                               updated_by_unit_code, updated_by_unit_description, updated_by_unit_display_name)
            SELECT fy.year                                  AS fiscal_year,
                   u.nuit                                   AS user_nuit,
                   u.name                                   AS user_full_name,
                   concat(u.nuit, ' - ', u.name)            AS user_display_name,
                   ap.name                                  as user_access_profile,
                   oc.code                                  AS user_unit_code,
                   oc.description                           AS user_unit_description,
                   concat(oc.code, ' - ', oc.description)   AS user_unit_display_name,
                   t.creationDate                           AS creation_date,
                   t.updateDate                             AS update_date,
                   if(t.active == 'true', 'Sim', 'Não')     AS active,
                   uc.nuit                                  AS created_by_nuit,
                   uc.name                                  AS created_by_full_name,
                   apc.description                          AS created_by_profile,
                   multiIf(t.methodName == 'updateUser', 'Actualização', t.methodName == 'setDeleted', 'Inativação',
                           t.methodName)                    AS action,
                   t.auditEntryCreationDate                 AS log_creation_date,
                   t.serverHostAddress                      AS server_host_address,
                   t.clientHostAddress                      AS client_host_address,
                   t.sessionId                              AS session_id,
                   uu.nuit                                  AS updated_by_nuit,
                   uu.name                                  AS updated_by_full_name,
                   concat(u.nuit, ' - ', uu.name)           AS updated_by_display_name,
                   apu.description                          AS updated_by_profile,
                   ocu.code                                 AS updated_by_unit_code,
                   ocu.description                          AS updated_by_unit_description,
                   concat(ocu.code, ' - ', ocu.description) AS updated_by_unit_display_name
            FROM temp_user_updates_log t
                     INNER JOIN dim_fiscal_year fy ON fy.source_id == t.fiscalYearId
                     INNER JOIN dim_user u ON u.engine_user_id == t.userId
                     INNER JOIN dim_access_profile ap ON ap.source_id == t.accessProfileId
                     INNER JOIN dim_organic_classifier oc ON oc.source_id = u.organic_classifier_id
                     LEFT JOIN dim_user uc ON uc.engine_user_id == t.createdBy
                     LEFT JOIN dim_access_profile apc ON apc.source_id == uc.access_profile_id
                     LEFT JOIN dim_user uu ON uu.engine_user_id = t.updatedBy
                     LEFT JOIN dim_access_profile apu ON apu.source_id == uu.access_profile_id
                     LEFT JOIN dim_organic_classifier ocu ON ocu.source_id == uu.organic_classifier_id;
            """

            logging.info('Loading data into fact_user_history table')
            clickhouse_client.command(insert_query)
            logging.info('Data successfully loaded')
            logging.info('Deleting temporary data from temp_user_updates_log')
            clickhouse_client.command("DELETE FROM temp_user_updates_log WHERE 1 = 1")
            logging.info('Temporary data successfully deleted')
        else:
            logging.info('No data found on the temp table')

    audit_task = SourceDataAuditOperator(task_id='extract_audit_task',
                                         source_dag_id='fact_user_history_etl_dag')

    extract_task() >> audit_task >> transform_and_load()


fact_user_history_etl_dag()
