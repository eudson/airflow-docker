import logging

from datetime import datetime

import numpy as np
from airflow.decorators import dag, task
from airflow.providers.oracle.hooks.oracle import OracleHook
from common.audit import SourceDataAuditOperator, push_audit_xcom_values, get_last_audit_info_record_last_id
from common.utils import get_clickhouse_client, format_date, DATE_FORMAT
import pandas as pd


@dag(
    schedule='@daily',
    catchup=False,
    start_date=datetime.now(),
    tags=["facts"],
)
def fact_user_status_history_etl_dag():
    @task()
    def oracle_to_clickhouse(**context):
        context["ti"].xcom_push(key="exec_date_start", value=datetime.now().strftime('%Y-%m-%d %X'))

        source = 'user_status_history'

        oracle_hook = OracleHook(oracle_conn_id='oracle_default')

        clickhouse_client = get_clickhouse_client()

        logging.info('Cleaning the table before')
        clickhouse_client.command('DELETE FROM fact_user_status_history WHERE 1 = 1')
        logging.info('Table cleaned successfully')

        query = f"""
        SELECT
            fy.YEAR AS fiscal_year,
            eu.ID AS user_id,
            ee.NUIT AS user_nuit,
            ee.NAME AS user_full_name,
            CONCAT(CONCAT(ee.NUIT, ' - '), ee.NAME) AS user_display,
            ap.CODE AS user_access_profile_code,
            ap.NAME AS user_access_profile_description,
            CONCAT(CONCAT(ap.CODE, ' - '), ap.NAME) AS user_access_profile_display,
            un.CODE AS user_organic_unit_code,
            un.DESCRIPTION AS user_organic_unit_description,
            CONCAT(CONCAT(un.CODE, ' - '), un.DESCRIPTION) AS user_organic_unit_display,
            al.CODE AS user_access_level,
            eu.CREATION_DATE AS creation_date,
            eu.UPDATE_DATE AS updated_date,
            eu.ACTIVATION_DATE AS activation_date,
            CASE 
                WHEN eu.ACTIVE = '1' THEN 'Sim' 
                WHEN eu.ACTIVE = '0' THEN 'Não' 
                ELSE eu.ACTIVE 
            END AS active,
            creu.ID AS created_by_id,
            cree.NUIT AS created_by_nuit,
            cree.NAME AS created_by_full_name,
            CONCAT(CONCAT(cree.NUIT, ' - '), cree.NAME) AS created_by_display,
            crap.CODE AS created_by_access_profile_code,
            crap.NAME AS created_by_access_profile_description,
            CONCAT(CONCAT(crap.CODE, ' - '), crap.NAME) AS created_by_access_profile_display,
            upeu.ID AS updated_by_id,
            upee.NUIT AS updated_by_nuit,
            upee.NAME AS updated_by_full_name,
            CONCAT(CONCAT(upee.NUIT, ' - '), upee.NAME) AS updated_by_display,
            upap.CODE AS updated_by_access_profile_code,
            upap.NAME AS updated_by_access_profile_description,
            CONCAT(CONCAT(upap.CODE, ' - '), upap.NAME) AS updated_by_access_profile_display,
            aceu.ID AS activated_by_id,
            acee.NUIT AS activated_by_nuit,
            acee.NAME AS activated_by_full_name,
            CONCAT(CONCAT(acee.NUIT, ' - '), acee.NAME) AS activated_by_display,
            acap.CODE AS activated_by_access_profile_code,
            acap.NAME AS activated_by_access_profile_description,
            CONCAT(CONCAT(acap.CODE, ' - '), acap.NAME) AS activated_by_access_profile_display
        FROM
            mex.ENGINE_USER eu
            INNER JOIN mex.USER_PASSWORD up ON up.ID = eu.USER_PASSWORD_ID
            INNER JOIN mex.EXTERNAL_ENTITY ee ON ee.NUIT = up.USER_NUIT
            INNER JOIN mex.ACCESS_PROFILE ap ON ap.ID = eu.ACCESS_PROFILE_ID
            INNER JOIN mex.ACCESS_LEVEL al ON al.ID = eu.ACCESS_LEVEL_ID
            INNER JOIN mex.UNIT un ON un.ID = eu.UNIT_ID
            LEFT JOIN mex.FISCAL_YEAR fy ON fy.ID = eu.FISCAL_YEAR_ID
            LEFT JOIN mex.ENGINE_USER creu ON creu.ID = eu.CREATED_BY
            LEFT JOIN mex.USER_PASSWORD crup ON crup.ID = creu.USER_PASSWORD_ID
            LEFT JOIN mex.EXTERNAL_ENTITY cree ON cree.NUIT = crup.USER_NUIT
            LEFT JOIN mex.ACCESS_PROFILE crap ON crap.ID = creu.ACCESS_PROFILE_ID
            LEFT JOIN mex.ENGINE_USER upeu ON upeu.ID = eu.UPDATED_BY
            LEFT JOIN mex.USER_PASSWORD upup ON upup.ID = upeu.USER_PASSWORD_ID
            LEFT JOIN mex.EXTERNAL_ENTITY upee ON upee.NUIT = upup.USER_NUIT
            LEFT JOIN mex.ACCESS_PROFILE upap ON upap.ID = upeu.ACCESS_PROFILE_ID
            LEFT JOIN mex.ENGINE_USER aceu ON aceu.ID = eu.ACTIVATED_BY
            LEFT JOIN mex.USER_PASSWORD acup ON acup.ID = aceu.USER_PASSWORD_ID
            LEFT JOIN mex.EXTERNAL_ENTITY acee ON acee.NUIT = acup.USER_NUIT
            LEFT JOIN mex.ACCESS_PROFILE acap ON acap.ID = aceu.ACCESS_PROFILE_ID
        ORDER BY
            ee.NUIT, eu.FISCAL_YEAR_ID ASC
        """
        logging.info('Fetching data from Oracle DB')
        results = oracle_hook.get_pandas_df(query)
        logging.info(f'Data successfully fetched, found {len(results)} records')

        column_names = [
            'fiscal_year', 'user_id', 'user_nuit', 'user_full_name', 'user_display',
            'user_access_profile_code', 'user_access_profile_description',
            'user_access_profile_display', 'user_organic_unit_code', 'user_organic_unit_description',
            'user_organic_unit_display', 'user_access_level', 'creation_date', 'updated_date',
            'activation_date', 'active', 'created_by_id', 'created_by_nuit', 'created_by_full_name',
            'created_by_display', 'created_by_access_profile_code',
            'created_by_access_profile_description', 'created_by_access_profile_display',
            'updated_by_id', 'updated_by_nuit', 'updated_by_full_name', 'updated_by_display',
            'updated_by_access_profile_code', 'updated_by_access_profile_description',
            'updated_by_access_profile_display', 'activated_by_id', 'activated_by_nuit',
            'activated_by_full_name', 'activated_by_display', 'activated_by_access_profile_code',
            'activated_by_access_profile_description', 'activated_by_access_profile_display'
        ]

        # CLEANING

        # Converting dates to a friendly format
        results['CREATION_DATE'] = pd.to_datetime(results['CREATION_DATE'], utc=True)
        results['UPDATED_DATE'] = pd.to_datetime(results['UPDATED_DATE'], utc=True)
        results['ACTIVATION_DATE'] = pd.to_datetime(results['ACTIVATION_DATE'], utc=True)
        logging.info(results['UPDATED_BY_ID'].unique())

        # Remove None and making sure we have Ints
        results['UPDATED_BY_ID'] = results['UPDATED_BY_ID'].fillna(0).astype(int)
        results['ACTIVATED_BY_ID'] = results['ACTIVATED_BY_ID'].fillna(0).astype(int)

        # To remove any nan or NaT types from the dataframe otherwise the insert will not work
        results.replace({np.nan: None}, inplace=True)

        list_to_insert = results.to_records(index=False)

        logging.info('Inserting data to Clickhouse')
        clickhouse_client.insert('fact_user_status_history', list_to_insert, column_names=column_names)
        logging.info('Completed!!!')

    oracle_to_clickhouse()


fact_user_status_history_etl_dag()
