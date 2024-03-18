import logging

import clickhouse_connect
from datetime import datetime
from airflow.decorators import dag, task


@dag(
    schedule='@daily',
    start_date=datetime(2024, 2, 18),
    catchup=False,
    tags=["Testing"],
)
def clickhouse_conn_test():
    @task()
    def test_connection():
        try:
            client = clickhouse_connect.get_client(host='host.docker.internal', port=55002)
        except:
            logging.error('Error trying to connect to clickhouse')
