# Start with the official Airflow image
FROM apache/airflow:2.7.1

# Switch to root to install git (required by dbt to download packages)
USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

# Switch back to airflow user to install Python packages
USER airflow

# Install dbt for Snowflake and the Airflow provider
RUN pip install --no-cache-dir \
    dbt-snowflake \
    dbt-core \
    apache-airflow-providers-snowflake