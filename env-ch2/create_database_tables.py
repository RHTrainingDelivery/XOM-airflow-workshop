import os
from airflow.sdk import dag,task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
@dag()
def create_database_tables():
    create_weather_temp_table = SQLExecuteQueryOperator(
        task_id="create_weather_temp_table",
        conn_id="tutorial_pg_conn",
        sql="sql/temp_weather_table.sql"
    )
    
    create_weather_table = SQLExecuteQueryOperator(
        task_id="create_weather_table",
        conn_id="tutorial_pg_conn",
        sql="sql/weather_table.sql"
    )
    
    create_weather_temp_table >> create_weather_table
        
create_database_tables()
