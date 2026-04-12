import os
import mysql.connector
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

password = os.getenv("MYSQL_PASSWORD")

print("Testing MySQL connection...")

# Test 1: Basic mysql-connector
try:
    conn = mysql.connector.connect(
        host='localhost',
        port=3306,
        user='root',
        password=password,
        database='stock_data'
    )
    print("✅ Basic MySQL connection works")
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print(f"Tables in database: {tables}")
    cursor.close()
    conn.close()

except Exception as e:
    print(f"❌ Basic connection failed: {e}")

# Test 2: SQLAlchemy connection
try:
    connection_string = f"mysql+mysqlconnector://root:{password}@localhost:3306/stock_data"
    engine = create_engine(connection_string)
    connection = engine.connect()
    print("✅ SQLAlchemy connection works — your DAG should work!")
    connection.close()
    engine.dispose()

except Exception as e:
    print(f"❌ SQLAlchemy connection failed: {e}")