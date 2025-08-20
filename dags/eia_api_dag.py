# eia_api_dag.py
"""
Airflow DAG to pull data from the EIA API and print it.
Later, this will be extended to transform and store the data in Postgres or CSV.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from sqlalchemy import create_engine
import os

# Environment variables (from your .env or docker-compose)
EIA_API_KEY = os.environ.get("EIA_API_KEY")
POSTGRES_CONN = os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN")

def fetch_eia_data(endpoint: str, params: dict):
    """Fetch data from EIA v2 API with pagination."""
    all_data = []
    offset = 0
    length = 1000  # batch size
    while True:
        params.update({"api_key": EIA_API_KEY, "offset": offset, "length": length})
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json().get("response", {}).get("data", [])
        if not data:
            break
        all_data.extend(data)
        offset += length
    return pd.DataFrame(all_data)

def load_to_postgres(df: pd.DataFrame, table_name: str):
    engine = create_engine(POSTGRES_CONN)
    df.to_sql(table_name, engine, if_exists="replace", index=False)

def etl_retail_sales():
    endpoint = "https://api.eia.gov/v2/electricity/retail-sales/data/"
    params = {
        "frequency": "monthly",
        "data[0]": "customers",
        "data[1]": "price",
        "data[2]": "revenue",
        "data[3]": "sales",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc"
    }
    df = fetch_eia_data(endpoint, params)
    load_to_postgres(df, "retail_sales")

def etl_generation():
    endpoint = "https://api.eia.gov/v2/electricity/series/data/"
    params = {
        "frequency": "monthly",
        "data[0]": "value",
        "facets[seriesId][]": "ELEC.GEN.ALL-US-99.M"  # adjust seriesId as needed
    }
    df = fetch_eia_data(endpoint, params)
    load_to_postgres(df, "generation")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="eia_electricity_etl",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
    tags=["eia", "electricity"]
) as dag:

    task_retail_sales = PythonOperator(
        task_id="etl_retail_sales",
        python_callable=etl_retail_sales
    )

    task_generation = PythonOperator(
        task_id="etl_generation",
        python_callable=etl_generation
    )

    task_retail_sales >> task_generation
