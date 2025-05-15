FROM apache/airflow:2.5.0

USER root
RUN apt-get update && apt-get install -y gcc build-essential

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"



