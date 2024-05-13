-- Run function creation with super_user
DROP FUNCTION get_month_name;
CREATE FUNCTION IF NOT EXISTS get_month_name
    ON CLUSTER cluster1 AS(month_code) ->
    CASE month_code
        WHEN '01' THEN 'Janeiro'
        WHEN '02' THEN 'Fevereiro'
        WHEN '03' THEN 'Março'
        WHEN '04' THEN 'Abril'
        WHEN '05' THEN 'Maio'
        WHEN '06' THEN 'Junho'
        WHEN '07' THEN 'Julho'
        WHEN '08' THEN 'Agosto'
        WHEN '09' THEN 'Setembro'
        WHEN '10' THEN 'Outubro'
        WHEN '11' THEN 'Novembro'
        WHEN '12' THEN 'Dezembro'
        ELSE 'N/A'
        END;

DROP TABLE IF EXISTS scda_analysis.etl_record_error_log ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.etl_source_data_audit ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.temp_user_updates_log ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.dim_fiscal_year ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.dim_access_profile ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.dim_access_level ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.dim_user ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.dim_transaction_type ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.dim_application ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.dim_organic_classifier ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.fact_user_history ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.fact_user_transaction_log ON CLUSTER cluster1 SYNC;
DROP TABLE IF EXISTS scda_analysis.fact_user_status_history ON CLUSTER cluster1 SYNC;

CREATE TABLE IF NOT EXISTS scda_analysis.etl_record_error_log ON CLUSTER cluster1
(
    id             UUID     DEFAULT generateUUIDv4(),
    source         String,
    source_id      String,
    error_message  String,
    timestamp      DateTime DEFAULT now(),
    record_payload String
) ENGINE = ReplicatedMergeTree
      ORDER BY (id, timestamp);

CREATE TABLE IF NOT EXISTS scda_analysis.etl_source_data_audit ON CLUSTER cluster1
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
    PRIMARY                   KEY( id)
) ENGINE = ReplicatedMergeTree
      ORDER BY (id, exec_date_end);

CREATE TABLE IF NOT EXISTS scda_analysis.temp_user_updates_log
(
    auditEntryId           UInt32,
    auditEntryCreationDate DateTime,
    updateDate             DateTime,
    updatedBy              UInt32,
    userDetailsId          UInt32,
    accessProfileId        UInt32,
    active                 String,
    activatedBy            UInt32,
    creationDate           DateTime,
    fiscalYearId           UInt32,
    accessLevelId          UInt32,
    createdBy              UInt32,
    entityName             String,
    publicEmployeeId       UInt32,
    userCreationRuleId     UInt32,
    action                 String,
    unitId                 UInt32,
    state                  UInt8,
    activationDate         DateTime,
    userPasswordId         UInt32,
    serverHostAddress      String,
    clientHostAddress      String,
    sessionId              String,
    transactionCode        UInt32,
    userName               String,
    applicationCode        String,
    userId                 UInt32,
    methodCode             UInt8,
    methodName             String
) ENGINE = MergeTree
      ORDER BY (auditEntryId);

CREATE TABLE IF NOT EXISTS scda_analysis.temp_user_transaction_log
(
    auditEntryId           UInt32,
    auditEntryCreationDate DateTime,
    createdBy              UInt32,
    serverHostAddress      String,
    clientHostAddress      String,
    transactionCode        String,
    applicationCode        String,
    year                   UInt16,
    month                  UInt8
) ENGINE = MergeTree
      ORDER BY (auditEntryId);


CREATE TABLE IF NOT EXISTS scda_analysis.dim_fiscal_year ON CLUSTER cluster1
(
    fiscal_year_id UUID DEFAULT generateUUIDv4(),
    source_id      UInt32,
    year           UInt16,
    PRIMARY        KEY (fiscal_year_id)
) ENGINE = ReplicatedMergeTree
      ORDER BY (fiscal_year_id, year);

CREATE TABLE IF NOT EXISTS scda_analysis.dim_access_profile ON CLUSTER cluster1
(
    access_profile_id UUID DEFAULT generateUUIDv4(),
    code              String,
    name              String,
    description       String,
    source_id         UInt32,
    PRIMARY           KEY (access_profile_id)
) ENGINE = ReplicatedMergeTree
      ORDER BY (access_profile_id, source_id);

CREATE TABLE IF NOT EXISTS scda_analysis.dim_access_level ON CLUSTER cluster1
(
    access_level_id UUID DEFAULT generateUUIDv4(),
    code            String,
    description     String,
    source_id       UInt32,
    PRIMARY         KEY (access_level_id)
) ENGINE = ReplicatedMergeTree
      ORDER BY (access_level_id, source_id);

CREATE TABLE IF NOT EXISTS scda_analysis.dim_user ON CLUSTER cluster1
(
    user_id               UUID DEFAULT generateUUIDv4(),
    access_level_id       UInt32,
    access_profile_id     UInt32,
    fiscal_year_id        UInt32,
    unit_id               UInt32,
    engine_user_id        UInt32,
    nuit                  String,
    name                  String,
    organic_classifier_id UInt32,
    PRIMARY               KEY (user_id)
) ENGINE = ReplicatedMergeTree
      ORDER BY (user_id, engine_user_id);

CREATE TABLE IF NOT EXISTS scda_analysis.dim_transaction_type ON CLUSTER cluster1
(
    transaction_type_id UUID DEFAULT generateUUIDv4(),
    code                String,
    description         String,
    fiscal_year_id      UInt32,
    application_id      UInt32,
    source_id           UInt32,
    PRIMARY             KEY (transaction_type_id)
) ENGINE = ReplicatedMergeTree
      ORDER BY (transaction_type_id, code);

CREATE TABLE IF NOT EXISTS scda_analysis.dim_application ON CLUSTER cluster1
(
    application_id UUID DEFAULT generateUUIDv4(),
    code           String,
    description    String,
    source_id      UInt32,
    PRIMARY        KEY (application_id)
) ENGINE = ReplicatedMergeTree
      ORDER BY (application_id, code);

CREATE TABLE IF NOT EXISTS scda_analysis.dim_organic_classifier ON CLUSTER cluster1
(
    organic_classifier_id            UUID DEFAULT generateUUIDv4(),
    source_id                        UInt64,
    fiscal_year_id                   UInt32,
    code                             String,
    description                      String,
    territory_classifier_description String
) ENGINE = ReplicatedMergeTree
      ORDER BY (organic_classifier_id, code);


CREATE TABLE IF NOT EXISTS scda_analysis.fact_user_history ON CLUSTER cluster1
(
    user_history_id              UUID DEFAULT generateUUIDv4(),
    fiscal_year                  UInt16,
    user_nuit                    String,
    user_full_name               String,
    user_display_name            String,
    user_profile                 String,
    user_unit_code               String,
    user_unit_description        String,
    user_unit_display_name       String,
    creation_date                DateTime,
    update_date                  DateTime,
    active                       String,
    created_by_nuit              String,
    created_by_full_name         String,
    created_by_profile           String,
    updated_by_nuit              String,
    updated_by_full_name         String,
    updated_by_display_name      String,
    updated_by_profile           String,
    updated_by_unit_description  String,
    updated_by_unit_code         String,
    updated_by_unit_display_name String,
    action                       String,
    log_creation_date            DateTime,
    server_host_address          String,
    client_host_address          String,
    session_id                   String
) ENGINE = ReplicatedMergeTree
      ORDER BY (user_history_id, log_creation_date);

CREATE TABLE IF NOT EXISTS scda_analysis.fact_user_transaction_log ON CLUSTER cluster1
(
    user_transaction_log_id         UUID DEFAULT generateUUIDv4(),
    log_creation_date               DateTime,
    user_id                         UInt32,
    user_nuit                       String,
    user_full_name                  String,
    user_display                    String,
    user_access_profile_code        String,
    user_access_profile_description String,
    user_access_profile_display     String,
    user_organic_unit_code          String,
    user_organic_unit_description   String,
    user_organic_unit_display       String,
    transaction_code                UInt32,
    transaction_description         String,
    transaction_display             String,
    application_code                String,
    application_name                String,
    application_display             String,
    fiscal_year                     UInt16,
    month                           UInt8,
    month_display                   String,
    month_year                      String,
    month_year_display              String,
    server_host_address             String,
    audit_entry_id                  UInt32,
    client_host_address             String
) ENGINE = ReplicatedMergeTree
      ORDER BY (user_transaction_log_id, log_creation_date);

CREATE TABLE IF NOT EXISTS fact_user_status_history ON CLUSTER cluster1
(
    user_status_history_id                  UUID DEFAULT generateUUIDv4(),
    fiscal_year                             UInt16,
    user_id                                 UInt32,
    user_nuit                               String,
    user_full_name                          String,
    user_display                            String,
    user_access_profile_code                String,
    user_access_profile_description         String,
    user_access_profile_display             String,
    user_organic_unit_code                  String,
    user_organic_unit_description           String,
    user_organic_unit_display               String,
    user_access_level                       String,
    creation_date                           Nullable(DateTime),
    updated_date                            Nullable(DateTime),
    activation_date                         Nullable(DateTime),
    active                                  String,
    created_by_id                           UInt32 DEFAULT 0,
    created_by_nuit                         String,
    created_by_full_name                    String,
    created_by_display                      String,
    created_by_access_profile_code          String,
    created_by_access_profile_description   String,
    created_by_access_profile_display       String,
    updated_by_id                           Nullable(UInt32),
    updated_by_nuit                         Nullable(String),
    updated_by_full_name                    Nullable(String),
    updated_by_display                      Nullable(String),
    updated_by_access_profile_code          Nullable(String),
    updated_by_access_profile_description   Nullable(String),
    updated_by_access_profile_display       Nullable(String),
    activated_by_id                         Nullable(UInt32),
    activated_by_nuit                       Nullable(String),
    activated_by_full_name                  Nullable(String),
    activated_by_display                    Nullable(String),
    activated_by_access_profile_code        Nullable(String),
    activated_by_access_profile_description Nullable(String),
    activated_by_access_profile_display     Nullable(String)
) ENGINE = ReplicatedMergeTree
      ORDER BY (user_status_history_id, fiscal_year);
