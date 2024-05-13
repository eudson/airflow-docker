import getpass
import logging

import oracledb
import numpy as np
import pandas as pd
from pandas.io import sql as psql
import clickhouse_connect
import sys
from dags.common.config_reader import ConfigurationReader

host = 'localhost'
port = 55002  # Replace with your ClickHouse port
database = 'scda_analysis'
user = 'deploy'
password = 'deploy'

# Create a ClickHouse client
print(f" Reading configuration from f{ConfigurationReader().environment}")
client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password, database=database)

print("connected to " + host + "\n")

un = 'mex'
cs = 'localhost/xe'
un = 'deploy'
cs = '172.31.4.131/SPRJDEV'
# pw = getpass.getpass(f'Enter password for {un}@{cs}: ')
pw = 'deploy'

connection = oracledb.connect(user=un, password=pw, dsn=cs)
sql = """SELECT
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
            WHERE 
                ROWNUM <= 100000
        ORDER BY
            ee.NUIT, eu.FISCAL_YEAR_ID ASC"""
results = psql.read_sql(sql=sql, con=connection)
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
results['CREATION_DATE'] = pd.to_datetime(results['CREATION_DATE'], utc=True)
results['UPDATED_DATE'] = pd.to_datetime(results['UPDATED_DATE'], utc=True)
results['ACTIVATION_DATE'] = pd.to_datetime(results['ACTIVATION_DATE'], utc=True)
logging.info(results['UPDATED_BY_ID'].unique())

results['UPDATED_BY_ID'] = results['UPDATED_BY_ID'].fillna(0).astype(int)
results['ACTIVATED_BY_ID'] = results['ACTIVATED_BY_ID'].fillna(0).astype(int)

i = 0
for u in results['UPDATED_BY_ID'].unique():
    logging.info(f'Index {i} : {u}')
    i = i + 1
        # To remove any nan or NaT types from the dataframe otherwise the insert will not work
results.replace({np.nan: None}, inplace=True)
logging.info(results['UPDATED_BY_ID'].dtypes)
logging.info(results['UPDATED_BY_ID'].unique())
list_to_insert = results.to_records(index=False)

logging.info('Inserting data to Clickhouse')
client.insert('fact_user_status_history', list_to_insert, column_names=column_names)
logging.info('Completed!!!')
print(results.iloc[0])
