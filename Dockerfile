FROM apache/airflow:3.0.6-python3.11

USER airflow

COPY airflow/requirements-airflow.txt /requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

