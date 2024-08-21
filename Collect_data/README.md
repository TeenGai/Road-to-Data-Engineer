# Data Collection
In this section, I will explain what I have learned about collecting data from various sources during my very first workshop in my Data Engineering career.

**Let's dive into the very first step!** :smile:
## Step 1: Import Required Libraries
```
import os
from dotenv import load_dotenv
import mysql.connector
import requests
import pandas as pd
```
This step is to import all the necessary libraries to collect data from both `MySQL Database` and `API`. I decided to use `Pandas DataFrame` to easily display tables, so the pandas package was imported as well.

## Load Vnvironment variables from .env File

Next step is to load environment variables, including `HOST, USER, PASSWORD, DATABASE, and PORT` from `.env` file that I have created on my local device.

Here's the code. Applying command `os.getenv` from the OS package to achieve this.

```
load_dotenv()

host=os.getenv("MYSQL_HOST")
user=os.getenv("MYSQL_USER")
password=os.getenv("MYSQL_PASSWORD")
database=os.getenv("MYSQL_DB")
port=os.getenv("MYSQL_PORT")
```

> ***Reference:** https://docs.python.org/3/library/os.html*

## Create a Connection to MySQL Database
```
db = mysql.connector.connect(
  host=host,
  user=user,
  password=password,
  database=database,
  port=port
)
```
Using command `mysql.connector.connect()` from mysql.connector package and apply variables from the second step to establish a connection to the MySQL database.

> ***Reference:** https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html*

## Create a Cursor to Execute Database Queries
```
cursor = db.cursor()

def execute_query(sql):
    cursor.execute(sql)
    return cursor.fetchall()

def show_tables():
    result = execute_query("SHOW TABLES")
    print(result)

def desc_tables():
    tables = ["customer", "product", "transaction"]
    for table in tables:
        result = execute_query(f"DESCRIBE {table}")
        print(f"-- {table} --")
        print(result)
        print()
```
In this step, I created a cursor by using `db.cursor()` command to execute queries from the database, along with defining three additional functions: 

1. `execute_query()` this function will be mostly applied together with the second and third functions to show all the tables in the database. With the `'sql'` that I have set as a parameter, it will be useful as it is reuseable for multiple tables if any.

2. `show_tables()` this function will be used to display the table names.

Here's the result... :arrow_heading_down: :arrow_heading_down: :arrow_heading_down:
```
[('customer',), ('product',), ('transaction',)]
```

3. `desc_tables()` since there are 3 tables in the database, I created a list containing the table names and used a `for loop` to describe schema of each table.

Here's the result... :arrow_heading_down: :arrow_heading_down: :arrow_heading_down:
```
-- customer --
[('CustomerNo', 'double', 'YES', '', None, ''), ('Country', 'text', 'YES', '', None, ''), ('Name', 'text', 'YES', '', None, '')]

-- product --
[('ProductNo', 'text', 'YES', '', None, ''), ('ProductName', 'text', 'YES', '', None, '')]

-- transaction --
[('TransactionNo', 'text', 'YES', '', None, ''), ('Date', 'datetime', 'YES', '', None, ''), ('ProductNo', 'text', 'YES', '', None, ''), ('Price', 'double', 'YES', '', None, ''), ('Quantity', 'bigint(20)', 'YES', '', None, ''), ('CustomerNo', 'double', 'YES', '', None, '')]
```

And YES...we successfully got table schemas as shown in the picture below...

![schema](https://github.com/user-attachments/assets/d671a1f2-3d62-4dc4-b4f9-1c72f91c53eb)

> *Reference: Road to Data Engineer course by DataTH*

## Convert Tables to Pandas DataFrames
```
def to_df(table):
    result = execute_query(f"SELECT * FROM {table}")
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(result, columns=columns)
    return df
```

I defined a function named `to_df()` to convert all tables to Pandas DataFrames because it will be easier to delve into each table and also for further data cleaning steps.

At first, I got an issue where the columns were labeled with numbers (e.g.0, 1, 2) :sweat_smile:. So, I added this line `columns = [col[0] for col in cursor.description]` to ensure the DataFrame displays with proper column names, which are then used as parameters in the `pd.DataFrame` command in the next line.

## Get Conversion Rate from API
```
def api():
    url = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
    r = requests.get(url)
    result = r.json()
    df = pd.DataFrame(result)
    df['date'] = pd.to_datetime(df['date'])
    return df
```

Before getting into the last step, there is one more source where we need to collect data from, which is an `API`. I created a function called `api()` and cast the date column to a `date-format`, as the original format extracted from JSON was in `string-format`. It makes things easier when joining with other tables in the next step.

## Merge All Tables Together!
```
product = to_df("product")
customer = to_df("customer")
transaction = to_df("transaction")
conversion_rate = api()

# Merge transaction with product and customer
final_df = transaction.merge(product, how='left', on='ProductNo') \
                        .merge(customer, how='left', on='CustomerNo') \
                        .merge(conversion_rate, how='left', left_on='Date', right_on='date')
```

Finally, we reached the last step of this section. I defined four new variables, `product`, `customer`, `transaction`, `conversion_rate` as a Pandas DataFrames using the `to_df()` and `api()` functions.

Then merged all tables together using `.merge()` command to create a DataFrame as `final_df`. And that's it for this Data Collection section! :satisfied:

> *Reference: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html*
