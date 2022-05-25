import os
import sys
import json
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

# path configs
HOME_DIR=os.path.expanduser('~')
AIRFLOW_DIR=os.path.join(HOME_DIR, "airflow")
sys.path.append(AIRFLOW_DIR)

# local imports
from include.functions.processing_user import _processing_user

# args common to all the tasks within the DAG
default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG(dag_id='user_processing', schedule_interval='@daily',
        default_args=default_args,
        catchup=False,
        template_searchpath=os.path.join(AIRFLOW_DIR, "include")) as dag:
    
    # Define the tasks/operators
    # task 1: creating table
    creating_table = SqliteOperator(
        task_id="creating_table",
        sqlite_conn_id="db_sqlite",
        sql="sql/create_table_users.sql"
    )

    # task 2: check if the api is available
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="user_api",
        endpoint="api/"
    )

    # task 3: fetch user data from api
    extracting_user = SimpleHttpOperator(
        task_id="extracting_user",
        http_conn_id="user_api",
        endpoint="api/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    # task 4: process user data
    processing_user = PythonOperator(
        task_id="processing_user",
        python_callable=_processing_user
    )

    # task 5: store user data
    storing_user = BashOperator(
        task_id="storing_user",
        bash_command="scripts/store_user.sh"
    )

    creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user
