import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.operators.oracle_operator import OracleOperator
import clickhouse_connect
import sys


# def test_mongo_conn():
#     print('Checking mongodb connection')
#     try:
#         hook = MongoHook(mongo_conn_id='mongo_default')
#         client = hook.get_conn()
#         print(f"Connected to mongo db ---  {client.server_info()}")
#         db = (client.auditlog)
#         print('Fetching data')
#         results = db.AuditEntry_01.find({
#             "payload.audit-data.audit-info.transactionCode": "995"
#             }).limit(2);
#         results = list(results)
#         print(f"Fetched successfully {len(results)} records")
#         for result in results:
#             print(result)
#             print(result['payload'])
#     except Exception as e:
#         print('Error connecting to the mongo database')
#         print(e)

def test_ch_conn():
    try:
        hook = ClickHouseHook(mongo_conn_id='clickhouse_conn')
        client = hook.get_conn()
        print('Checking clickhouse_connection')
        print(client)
    except Exception as e:
        print(e)


def test_oracle_conn():
    print('Checking oracle connection')
    try:
        hook = OracleHook(oracle_conn_id='oracle_conn_localhost')
        print(f"Oracle conn config --- {hook}")
        client = hook.get_conn()
        print(f"Connected to oracle successfully")
    except Exception as e:
        print('Error connecting oracle database')
        print(e)


def test_clickouse_conn_direct():
    host = 'host.docker.internal'
    port = 55002  # Replace with your ClickHouse port
    database = 'scda_analysis'
    user = 'deploy'
    password = 'deploy'

    # Create a ClickHouse client
    client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password, database=database)

    print("connected to " + host + "\n")

    # Example query: select data from the table
    select_query = 'CREATE TABLE IF NOT EXISTS new_table (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key'
    client.command(select_query)

    print("table new_table created or exists already!\n")

    row1 = [1000, 'String Value 1000', 5.233]
    row2 = [2000, 'String Value 2000', -107.04]
    data = [row1, row2]
    client.insert('new_table', data, column_names=['key', 'value', 'metric'])

    print("written 2 rows to table new_table\n")

    QUERY = "SELECT max(key), avg(metric) FROM new_table"

    result = client.query(QUERY)

    sys.stdout.write("query: [" + QUERY + "] returns:\n\n")
    print(result.result_rows)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'test_database_connections_v16',
    default_args=default_args,
    description='A DAG to test connections to MongoDB, Oracle, and ClickHouse',
    schedule_interval=timedelta(days=1),  # Adjust the interval as needed
)

# mongodb_task = PythonOperator(
#         task_id="test_mongo_conn",
#         python_callable=test_mongo_conn,
#         dag = dag
#     )

click_house_task = PythonOperator(
    task_id='test_clickhouse_conn_direct',
    python_callable=test_clickouse_conn_direct,
    dag=dag
)
# click_house_task = ClickhouseOperator(
#     task_id='test_clickhouse_conn',
#     click_conn_id='clickhouse_default',
#     sql='SELECT 1',
#     dag = dag
# )

oracle_test_task = PythonOperator(
    task_id="test_oracle_conn",
    python_callable=test_oracle_conn,
    dag=dag
)

# mongodb_task, 
# oracle_test_task, 
oracle_test_task
