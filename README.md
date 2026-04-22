# 📈 Stock Data ETL Pipeline

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![MySQL](https://img.shields.io/badge/mysql-%2300f.svg?style=for-the-badge&logo=mysql&logoColor=white)

An automated ETL pipeline that fetches stock data from RapidAPI and loads it into MySQL using Apache Airflow.

## 🚀 What it does

1. **Extract** - Fetches real-time stock data from RapidAPI
2. **Transform** - Cleans and processes the data using Python/Pandas  
3. **Load** - Stores the processed data in MySQL database

All automated and scheduled using Apache Airflow!

https://github.com/user-attachments/assets/8aa54282-0e80-4999-9bd5-c48d1c188bdd

## 🏗️ System Architecture

![System Architecture](HLD.png)

## 🛠️ Tech Stack

- **Apache Airflow** - Orchestrates the entire pipeline and handles scheduling
- **Python** - Core language with pandas for data processing
- **RapidAPI** - Source for real-time stock market data
- **MySQL** - Database for storing processed stock data
- **Docker** - Containerization for easy deployment

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
│   └── my_dag.py              # Main ETL pipeline
├── docker-compose.yml         # Docker configuration  
├── .gitignore                 # Git ignore file
└── README.md                  # This file
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
