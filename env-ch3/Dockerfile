FROM registry.ocp4.example.com:8443/apache/airflow:3.0.3
# Exchange the first line with the following one if you are running on your own environment
# FROM docker.io/apache/airflow:3.0.3

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

RUN python -m venv latest && source latest/bin/activate && pip install --no-cache-dir meteostat==1.7.4 apache-airflow==${AIRFLOW_VERSION} && deactivate