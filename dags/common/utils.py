from datetime import datetime
from airflow.models import Variable
import clickhouse_connect

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def cast_to_int(value):
    return int(value) if value else 0


def format_date(value):
    formatted_value = '1970-01-01 00:00:00' if value == '' else datetime.strptime(
        value, "%d/%m/%Y - %H:%M:%S.%f").strftime(DATE_FORMAT)

    return formatted_value


def get_clickhouse_client():
    return clickhouse_connect.get_client(host=Variable.get('clickhouse.host'),
                                         port=Variable.get('clickhouse.port'),
                                         username=Variable.get('clickhouse.username'),
                                         password=Variable.get('clickhouse.password'),
                                         database=Variable.get('clickhouse.db'))
