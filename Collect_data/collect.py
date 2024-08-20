import os
from dotenv import load_dotenv
import mysql.connector
import requests
import pandas as pd

# Load environment variables from .env file
load_dotenv()

host=os.getenv("MYSQL_HOST")
user=os.getenv("MYSQL_USER")
password=os.getenv("MYSQL_PASSWORD")
database=os.getenv("MYSQL_DB")
port=os.getenv("MYSQL_PORT")


# Create connection
db = mysql.connector.connect(
  host=host,
  user=user,
  password=password,
  database=database,
  port=port
)


# Create Cursor
cursor = db.cursor()


# Execute query
def execute_query(sql):
    cursor.execute(sql)
    return cursor.fetchall()


# Review tables
def show_tables():
    result = execute_query("SHOW TABLES")
    print(result)


# Describe tables
def desc_tables():
    tables = ["customer", "product", "transaction"]
    for table in tables:
        result = execute_query(f"DESCRIBE {table}")
        print(f"-- {table} --")
        print(result)
        print()


# Convert to Pandas DataFrame
def to_df(table):
    result = execute_query(f"SELECT * FROM {table}")
    columns = [col[0] for col in cursor.description] # To show header
    df = pd.DataFrame(result, columns=columns)
    return df


# Get data from API
def api():
    url = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
    r = requests.get(url)
    result = r.json()
    df = pd.DataFrame(result)
    df['date'] = pd.to_datetime(df['date'])
    return df


# DataFrame
product = to_df("product")
customer = to_df("customer")
transaction = to_df("transaction")
conversion_rate = api()


# Merge transaction with product and customer
final_df = transaction.merge(product, how='left', on='ProductNo') \
                        .merge(customer, how='left', on='CustomerNo') \
                        .merge(conversion_rate, how='left', left_on='Date', right_on='date')