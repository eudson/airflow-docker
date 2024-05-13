import logging

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.oracle.hooks.oracle import OracleHook
from common.audit import SourceDataAuditOperator, push_audit_xcom_values
from common.utils import get_clickhouse_client
from airflow.models import Variable

etl_record_error_log = """
    CREATE TABLE IF NOT EXISTS scda_analysis.etl_record_error_log
    (
        id            UUID     DEFAULT generateUUIDv4(),
        source        String,
        source_id     String,
        error_message String,
        record_payload String,
        timestamp     DateTime DEFAULT now()
    ) ENGINE = MergeTree()
          ORDER BY (id, timestamp);
"""

etl_source_data_audit = """
        CREATE TABLE IF NOT EXISTS scda_analysis.etl_source_data_audit
        (
            id                        UUID DEFAULT generateUUIDv4(),
            source                    String,
            last_record_creation_date DateTime,
            start_id                  UInt32,
            last_id                   UInt32,
            last_row_num              UInt32,
            exec_date_start           DateTime,
            exec_date_end             DateTime,
            total_records             UInt32,
            processed_records         UInt32,
            failed_records            UInt32,
            PRIMARY                   KEY(id)
        ) ENGINE = MergeTree()
              ORDER BY (id, exec_date_end);
        """
temp_user_updates_log = """
            CREATE TABLE IF NOT EXISTS scda_analysis.temp_user_updates_log (
                auditEntryId UInt32,
                auditEntryCreationDate DateTime,
                updateDate DateTime,
                updatedBy UInt32,
                userDetailsId UInt32,
                accessProfileId UInt32,
                active String,
                activatedBy UInt32,
                creationDate DateTime,
                fiscalYearId UInt32,
                accessLevelId UInt32,
                createdBy UInt32,
                entityName String,
                publicEmployeeId UInt32,
                userCreationRuleId UInt32,
                action String,
                unitId UInt32,
                state UInt8,
                id UInt32,
                activationDate DateTime,
                userPasswordId UInt32,
                serverHostAddress String,
                clientHostAddress String,
                sessionId String,
                transactionCode UInt32,
                userName String,
                applicationCode String,
                userId UInt32,
                methodCode UInt8,
                methodName String
            ) ENGINE = MergeTree()
            ORDER BY (auditEntryId)
            """

dim_fiscal_year = """
    CREATE TABLE IF NOT EXISTS scda_analysis.dim_fiscal_year (
        fiscal_year_id UUID DEFAULT generateUUIDv4(),
        source_id UInt32,
        year UInt16,
        PRIMARY KEY (fiscal_year_id)
    ) ENGINE = MergeTree()
    ORDER BY(fiscal_year_id, year);
    """

dim_access_profile = """
    CREATE TABLE IF NOT EXISTS  scda_analysis.dim_access_profile(
        access_profile_id UUID DEFAULT generateUUIDv4(),
        code String,
        name String,
        description String,
        source_id UInt32,
        PRIMARY KEY (access_profile_id)
    ) ENGINE = MergeTree()
    ORDER BY (access_profile_id, source_id)
    """

dim_access_level = """
    CREATE TABLE IF NOT EXISTS  scda_analysis.dim_access_level(
        access_level_id UUID DEFAULT generateUUIDv4(),
        code String,
        description String,
        source_id UInt32,
        PRIMARY KEY (access_level_id)
    ) ENGINE = MergeTree()
    ORDER BY (access_level_id, source_id)
    """

dim_user = """
    CREATE TABLE IF NOT EXISTS  scda_analysis.dim_user(
        user_id UUID DEFAULT generateUUIDv4(),
        access_level_id UInt32,
        access_profile_id UInt32,
        fiscal_year_id UInt32,
        unit_id UInt32,
        engine_user_id UInt32,
        nuit String,
        name String,
        organic_classifier_id UInt32,
        PRIMARY KEY (user_id)
    ) ENGINE = MergeTree()
    ORDER BY (user_id, engine_user_id)
    """

dim_transaction_type = """
    CREATE TABLE IF NOT EXISTS  scda_analysis.dim_transaction_type(
        transaction_type_id UUID DEFAULT generateUUIDv4(),
        code String,
        description String,
        fiscal_year_id UInt32,
        application_id UInt32,
        source_id UInt32,
        PRIMARY KEY (transaction_type_id)
    ) ENGINE = MergeTree()
    ORDER BY (transaction_type_id, code)
    """

dim_application = """
    CREATE TABLE IF NOT EXISTS  scda_analysis.dim_application(
        application_id UUID DEFAULT generateUUIDv4(),
        code String,
        description String,
        source_id UInt32,
        PRIMARY KEY (application_id)
    ) ENGINE = MergeTree()
    ORDER BY (application_id, code)
    """

fact_user_history = """
    CREATE TABLE IF NOT EXISTS scda_analysis.fact_user_history
    (
        user_history_id      UUID DEFAULT generateUUIDv4(),
        fiscal_year          UInt8,
        user_nuit            String,
        user_full_name       String,
        user_profile         String,
        organic_unit         String,
        creation_date        DateTime,
        update_date          DateTime,
        active               String,
        created_by_nuit      String,
        created_by_full_name String,
        created_by_profile   String,
        updated_by_nuit      String,
        updated_by_full_name String,
        updated_by_profile   String,
        action               String,
        log_creation_date    DateTime,
        server_host_address  String,
        client_host_address  String,
        session_id           String
    ) ENGINE = MergeTree()
          ORDER BY (user_history_id, log_creation_date);
    """

dim_organic_classifier = """
CREATE TABLE IF NOT EXISTS scda_analysis.dim_organic_classifier
(
    organic_classifier_id UUID DEFAULT generateUUIDv4(),
    source_id UInt64,
    fiscal_year_id UInt32,
    code String,
    description String,
    territory_classifier_description String
) ENGINE = MergeTree()
    ORDER BY (organic_classifier_id, code);
"""

fac_user_transaction_log = """
CREATE TABLE IF NOT EXISTS scda_analysis.fact_user_transaction_log
(
    user_transaction_log_id UUID DEFAULT generateUUIDv4(),
    log_creation_date       DateTime,
    user_id                 UInt32,
    user_nuit               String,
    user_full_name          String,
    user_display            String,
    transaction_code        UInt32,
    transaction_description String,
    transaction_display     String,
    application_code        String,
    application_name        String,
    application_display     String,
    year                    UInt8,
    month                   UInt8,
    server_host_address     String,
    audit_entry_id          UInt32,
    client_host_address     String
) ENGINE = MergeTree()
      ORDER BY (user_transaction_log_id, log_creation_date);
"""

tem_user_transaction_log = """
CREATE TABLE IF NOT EXISTS scda_analysis.temp_user_transaction_log
(
    auditEntryId           UInt32,
    auditEntryCreationDate DateTime,
    createdBy              UInt32,
    serverHostAddress      String,
    clientHostAddress      String,
    transactionCode        UInt32,
    applicationCode        String,
    year                   UInt8,
    month                  UInt8
) ENGINE = MergeTree
      ORDER BY (auditEntryId);
"""


@dag(
    schedule='@once',
    catchup=False,
    start_date=datetime.now(),
    tags=["ch_db_init"],
)
def clickhouse_db_init_dag():
    @task()
    def drop_all_tables():
        client = get_clickhouse_client()
        logging.info('Deleting all tables for database scda_analysis...')
        sql_commands = ['DROP TABLE IF EXISTS scda_analysis.dim_fiscal_year',
                        'DROP TABLE IF EXISTS scda_analysis.temp_user_updates_log',
                        'DROP TABLE IF EXISTS scda_analysis.etl_source_data_audit',
                        'DROP TABLE IF EXISTS scda_analysis.dim_access_profile',
                        'DROP TABLE IF EXISTS scda_analysis.dim_access_level',
                        'DROP TABLE IF EXISTS scda_analysis.dim_user',
                        'DROP TABLE IF EXISTS scda_analysis.dim_transaction_type',
                        'DROP TABLE IF EXISTS scda_analysis.dim_application',
                        'DROP TABLE IF EXISTS scda_analysis.etl_record_error_log',
                        'DROP TABLE IF EXISTS scda_analysis.fact_user_history',
                        'DROP TABLE IF EXISTS scda_analysis.dim_organic_classifier',
                        'DROP TABLE IF EXISTS scda_analysis.temp_user_transaction_log',
                        'DROP TABLE IF EXISTS scda_analysis.fact_user_transaction_log'

                        ]

        for command in sql_commands:
            client.command(command)

        logging.info('Tables deleted successfully')

    @task()
    def create_tables():
        logging.info('Creating tables...')
        client = get_clickhouse_client()
        sql_commands = [etl_source_data_audit,
                        temp_user_updates_log,
                        dim_user, dim_application,
                        dim_access_level, dim_access_profile,
                        dim_transaction_type, dim_organic_classifier,
                        dim_fiscal_year, etl_record_error_log, fact_user_history, tem_user_transaction_log,
                        fac_user_transaction_log]
        for command in sql_commands:
            client.command(command)

        logging.info('All tables created successfully')

    drop_all_tables() >> create_tables()


clickhouse_db_init_dag()
