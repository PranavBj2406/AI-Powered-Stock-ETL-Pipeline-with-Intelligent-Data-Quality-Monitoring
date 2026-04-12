# 📈 Stock Data ETL Pipeline

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![MySQL](https://img.shields.io/badge/mysql-%2300f.svg?style=for-the-badge&logo=mysql&logoColor=white)
![LangChain](https://img.shields.io/badge/LangChain-AI%20Agent-blue?style=for-the-badge)

An automated ETL pipeline that fetches stock data from RapidAPI and loads it into MySQL using Apache Airflow.

## 🚀 What it does

1. **Extract** - Fetches real-time stock data from RapidAPI
2. **Transform** - Cleans and processes the data using Python/Pandas  
3. **Validate (LangChain Agent)** - Uses an AI-powered quality agent to verify stock data integrity
4. **Load** - Stores validated data in MySQL database

All automated and scheduled using Apache Airflow!

https://github.com/user-attachments/assets/8aa54282-0e80-4999-9bd5-c48d1c188bdd

## 🤖 AI Data Quality Agent (LangChain)

This pipeline includes an intelligent validation layer powered by LangChain.

Before loading data into MySQL, a custom Stock Quality Agent:

- Detects missing or null values
- Validates price ranges
- Checks abnormal spikes or anomalies
- Ensures schema consistency
- Flags suspicious stock records before database insertion

This improves reliability of downstream analytics and prevents corrupt financial data from entering storage.

The agent runs automatically inside the Airflow DAG as part of the ETL workflow.


## 🛠️ Tech Stack

- **Apache Airflow** - Workflow orchestration and scheduling
- **Python** - Core pipeline implementation
- **Pandas** - Data transformation layer
- **LangChain** - AI-powered stock data quality validation agent
- **RapidAPI** - Real-time stock market data source
- **MySQL** - Persistent storage for processed stock data
- **Docker** - Containerized deployment environment

## 📋 Prerequisites

- Docker installed on your machine
- RapidAPI account (for stock data API)
- MySQL running locally

## 🔧 Setup

1. **Clone the repo**
```bash
git clone <your-repo-url>
cd stock-etl-pipeline
```
install packages based on requirements.txt
 
2. **Create environment file**
Create a `.env` file with your credentials:
```env
RAPIDAPI_KEY=your_api_key_here
```

3. **Start the pipeline**
```bash
docker-compose up -d
```

4. **Access Airflow**
- Go to `http://localhost:8080`
- Login: admin/admin
- Enable your DAG

## 📁 Project Structure

```
├── dags/
│   ├── my_dag.py
│   └── agents/
│       └── stock_quality_agent.py   # LangChain validation agent
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## 🔧 The Pipeline

**Extract Task** → **Transform Task** → **Load Task**

Each task runs in sequence, and if one fails, you can see the logs in Airflow UI to debug.

## 📊 Sample Output

The pipeline creates a `stock_prices` table in MySQL with columns like:
- symbol (e.g., AAPL, GOOGL)
- price 
- volume
- timestamp

## 🐛 Troubleshooting

**Can't connect to MySQL?**
- Make sure MySQL is running
- Use `host.docker.internal` instead of `localhost` in your connection string

**Task failing?**
- Check the logs in Airflow UI
- Click on the failed task → View Logs

## 📝 Notes

- The pipeline runs automatically based on your schedule
- All logs are available in Airflow web interface
- Data gets appended to MySQL table on each run

---
