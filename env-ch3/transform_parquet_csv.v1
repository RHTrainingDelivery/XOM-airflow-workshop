import os
import pandas as pd
from urllib.request import urlretrieve
from airflow.sdk import dag, task
from airflow.sdk import dag,task,get_current_context

@dag
def transform_parquet_csv():
    
    @task
    def download_parquet_file():
        context=get_current_context()
        ti = context['ti']
        data_path = ti.xcom_pull(key="data_path", task_ids="initialize_variables")
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        url="https://github.com/RHTrainingDelivery/XOM-airflow-workshop/raw/refs/heads/main/weather.parquet"
        urlretrieve(url,data_path)

    @task
    def convert_parquet_file_to_csv():
        context=get_current_context()
        ti = context['ti']
        data_path = ti.xcom_pull(key="data_path", task_ids="initialize_variables")
        csv_path = ti.xcom_pull(key="csv_path", task_ids="initialize_variables")
        df=pd.read_parquet(data_path)
        df.to_csv(csv_path,index_label="Id")
    
            
    download_parquet_file() >> convert_parquet_file_to_csv()

transform_parquet_csv()
