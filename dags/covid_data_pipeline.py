from airflow.models import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from datetime import datetime


default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _download_data(ti, **kwargs):
    group = kwargs["group"]
    kind = kwargs["kind"]
    group = "US" if group == "usa" else "global"
    kind = "confirmed" if kind == "cases" else "deaths"

    DOWNLOAD_URL = (
        "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/"
        "master/csse_covid_19_data/csse_covid_19_time_series/"
        "time_series_covid19_{kind}_{group}.csv"
    )
    print(DOWNLOAD_URL.format(kind=kind, group=group))
    df = pd.read_csv(DOWNLOAD_URL.format(kind=kind, group=group))
    output_pathname = f"~/airflow/data/raw/{group}_{kind}.csv"
    df.to_csv(output_pathname, index=False)
    ti.xcom_push(key="path_to_downloaded_data", value=output_pathname)


def _select_columns(ti, **kwargs):
    group = kwargs["group"]
    kind = kwargs["kind"]
    task_id = kwargs["task_id"]
    input_pathname = ti.xcom_pull(key="path_to_downloaded_data", task_ids=[task_id][0])
    df = pd.read_csv(input_pathname)

    cols = df.columns
    areas = ["Country/Region", "Province_State"]
    is_area = cols.isin(areas)
    # date columns are the only ones with two slashes
    has_two_slashes = cols.str.count("/") == 2
    filt = is_area | has_two_slashes

    df = df.loc[:, filt]
    df.to_csv(f"~/airflow/data/prepared/{group}_{kind}.csv", index=False)



with DAG(dag_id='covid_data_pipeline', schedule_interval='@daily',
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
