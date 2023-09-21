FROM apache/airflow:2.7.1-python3.10
COPY requirements.txt /
RUN pip3 install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r/requirements.txt