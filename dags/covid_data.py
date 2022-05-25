import os
import sys
from airflow.models import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from datetime import datetime

# path configs
HOME_DIR=os.path.expanduser('~')
AIRFLOW_DIR=os.path.join(HOME_DIR, "airflow")
sys.path.append(AIRFLOW_DIR)

# local imports
from include.functions.covid_data import (
    _download_data,
    _select_columns
)

default_args = {
    'start_date': datetime(2020, 1, 1)
}


with DAG(dag_id='covid_data', schedule_interval='@daily',
        default_args=default_args,
        catchup=False) as dag:

    download_usa_deaths_data = PythonOperator(
        task_id="download_usa_deaths_data",
        python_callable=_download_data,
        op_kwargs={"group": "usa", "kind": "deaths"}
    )

    download_usa_cases_data = PythonOperator(
        task_id="download_usa_cases_data",
        python_callable=_download_data,
        op_kwargs={"group": "usa", "kind": "cases"}
    )

    download_world_deaths_data = PythonOperator(
        task_id="download_world_deaths_data",
        python_callable=_download_data,
        op_kwargs={"group": "world", "kind": "deaths"}
    )

    download_world_cases_data = PythonOperator(
        task_id="download_world_cases_data",
        python_callable=_download_data,
        op_kwargs={"group": "world", "kind": "cases"}
    )

    select_columns_usa_deaths_data = PythonOperator(
        task_id="select_columns_usa_deaths_data",
        python_callable=_select_columns,
        op_kwargs={"group": "usa", "kind": "deaths", "task_id": "download_usa_deaths_data"}
    )

    select_columns_usa_cases_data = PythonOperator(
        task_id="select_columns_usa_cases_data",
        python_callable=_select_columns,
        op_kwargs={"group": "usa", "kind": "cases", "task_id": "download_usa_cases_data"}
    )

    select_columns_world_deaths_data = PythonOperator(
        task_id="select_columns_world_deaths_data",
        python_callable=_select_columns,
        op_kwargs={"group": "world", "kind": "deaths", "task_id": "download_world_deaths_data"}
    )

    select_columns_world_cases_data = PythonOperator(
        task_id="select_columns_world_cases_data",
        python_callable=_select_columns,
        op_kwargs={"group": "world", "kind": "cases", "task_id": "download_world_cases_data"}
    )


    download_usa_deaths_data >> select_columns_usa_deaths_data
    download_usa_cases_data >> select_columns_usa_cases_data
    download_world_deaths_data >> select_columns_world_deaths_data
    download_world_cases_data >> select_columns_world_cases_data
