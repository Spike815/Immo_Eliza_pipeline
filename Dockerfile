FROM apache/airflow:2.7.1-python3.10
COPY requirements.txt /
RUN pip install --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r/requirements.txt