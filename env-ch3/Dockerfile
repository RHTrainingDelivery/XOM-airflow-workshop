FROM apache/airflow:3.0.3
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

RUN python -m venv latest && source latest/bin/activate && pip install --no-cache-dir meteostat==1.7.4 apache-airflow==${AIRFLOW_VERSION} && deactivate