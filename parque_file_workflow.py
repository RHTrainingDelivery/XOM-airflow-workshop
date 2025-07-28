import pandas as pd
import os
from airflow.sdk import dag

@dag()
def process_weather_info():
    @task
    def download_parquet_file():
        data_path = "/opt/airflow/dags/files/parquet_file.parquet"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

    @task(
        task_id="read_parquet_example"
    )
    def read_parquet_file():
        df=pd.read_parquet(filepath)
        return df
    
    download_parquet_file() >> read_parquet_file()
process_weather_info()
