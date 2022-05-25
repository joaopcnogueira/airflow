import pandas as pd

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
