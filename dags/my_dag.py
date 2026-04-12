import http.client
import json
import os
from airflow import DAG
from datetime import datetime
import pandas as pd
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from agents.stock_quality_agent import run_stock_quality_agent  # ← TOP

RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

def extract_stock_data(**context):
    conn = http.client.HTTPSConnection("alpha-vantage.p.rapidapi.com")
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': "alpha-vantage.p.rapidapi.com"
    }
    try:
        conn.request("GET", "/query?function=TIME_SERIES_DAILY&symbol=MSFT&outputsize=compact&datatype=json", headers=headers)
        res = conn.getresponse()
        raw = json.loads(res.read().decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"API request failed: {e}")
    if "Time Series (Daily)" not in raw:
        raise ValueError(f"Unexpected API response: {raw.get('Note') or raw.get('Information') or raw}")
    context['ti'].xcom_push(key='raw_stock_data', value=raw)
    print(f"Extracted {len(raw['Time Series (Daily)'])} records")

def transform_stock_data(**context):
    raw_data = context['ti'].xcom_pull(task_ids='extract_stock_data', key='raw_stock_data')
    if not raw_data:
        raise ValueError("No data received from extract task")
    time_series = raw_data['Time Series (Daily)']
    transformed_data = []
    dates = sorted(time_series.keys())
    for i, date in enumerate(dates):
        values = time_series[date]
        close = float(values['4. close'])
        prev_close = float(time_series[dates[i - 1]]['4. close']) if i > 0 else None
        record = {
            'date': date,
            'symbol': 'MSFT',
            'open_price': float(values['1. open']),
            'high_price': float(values['2. high']),
            'low_price': float(values['3. low']),
            'close_price': close,
            'volume': int(values['5. volume']),
            'price_change': round(close - prev_close, 4) if prev_close else None,
            'extracted_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        transformed_data.append(record)
    context['ti'].xcom_push(key='transformed_stock_data', value=transformed_data)
    print(f"Transformed {len(transformed_data)} records")

def load_stock_data(**context):
    data = context['ti'].xcom_pull(task_ids='transform_stock_data', key='transformed_stock_data')
    if not data:
        raise ValueError("No data received from transform task")
    connection_string = f"mysql+mysqlconnector://root:{MYSQL_PASSWORD}@host.docker.internal:3306/stock_data"
    df = pd.DataFrame(data)
    engine = create_engine(connection_string)
    try:
        with engine.connect() as conn:
            existing_dates = tuple(df['date'].tolist())
            conn.execute(
                f"DELETE FROM stock_prices WHERE date IN {existing_dates} AND symbol = 'MSFT'"
            )
        df.to_sql(name='stock_prices', con=engine, if_exists='append', index=False)
        print(f"Loaded {len(df)} records to MySQL")
    except Exception as e:
        raise RuntimeError(f"Database load failed: {e}")
    finally:
        engine.dispose()

def run_agent_task(**context):
    report = run_stock_quality_agent(symbol="MSFT")
    print("=" * 60)
    print("AGENT DATA QUALITY REPORT")
    print("=" * 60)
    print(report)
    context['ti'].xcom_push(key='quality_report', value=report)

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    "stock_etl_dag",
    start_date=datetime(2025, 6, 16),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_task = PythonOperator(task_id='extract_stock_data', python_callable=extract_stock_data)
    transform_task = PythonOperator(task_id='transform_stock_data', python_callable=transform_stock_data)
    load_task = PythonOperator(task_id='load_stock_data', python_callable=load_stock_data)
    agent_task = PythonOperator(task_id='run_quality_agent', python_callable=run_agent_task)

    extract_task >> transform_task >> load_task >> agent_task  # ← single chain