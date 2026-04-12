FROM apache/airflow:2.9.1-python3.10

USER root
RUN apt-get update && apt-get install -y gcc && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    langchain==0.1.20 \
    langchain-groq==0.1.3 \
    langchain-community==0.0.38 \
    langchain-core==0.1.53 \
    mysql-connector-python \
    sqlalchemy \
    pandas