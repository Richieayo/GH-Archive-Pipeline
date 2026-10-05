FROM apache/airflow:3.0.2

# databricks-sdk goes into Airflow's own environment because the DAG imports it
# at module level. It does not conflict with Airflow's pins.
RUN pip install --no-cache-dir databricks-sdk

# dbt lives in an isolated venv: dbt-core requires click>=8.3 while Airflow
# pins click<8.3, so installing dbt alongside Airflow breaks the Airflow CLI.
USER root

RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
       dbt-core==1.12.5 dbt-databricks==1.12.6 \
    && chown -R airflow: /opt/dbt-venv
USER airflow