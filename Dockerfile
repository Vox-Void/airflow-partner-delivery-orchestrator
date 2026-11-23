FROM apache/airflow:3.4.0-python3.12@sha256:818f94affc5bd7ce0be0cc04f2ace09aa8382e65190a73a08881229ff32700cb

USER airflow
RUN pip install --no-cache-dir --no-deps \
    "psycopg==3.3.6" "psycopg-binary==3.3.6" \
    "pytest==9.2.0" "iniconfig==2.3.1" "pluggy==1.7.0"

ENV PYTHONPATH=/opt/airflow/src
