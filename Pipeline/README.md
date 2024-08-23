# ETL Pipeline
I combined the steps and code from the `Collecting` and `Cleaning` sections to create an `ETL pipeline` in order to upload cleaned data to `BigQuery`.

**Let's dive into the first step!** :smile:
## Import Required Libraries
```
from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import requests
import pandas as pd
```
This step is to import all the necessary libraries for: 
- `MySqlHook` for collecting data from both 'MySQL Database' and 'Rest API', with the connection created in Airflow
- `Pandas DataFrame` for data cleaning
- `DAG`, `dag`, `task` for instantiating dag and create task
- `GCSToBigQueryOperator` for uploading cleaned data from GCS to BigQuery
- `days_ago`

## Define URL, Path, and Default Arguments
```
CONVERSION_RATE_URL = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"
```
:arrow_up: I defined `API URL` to get conversion rate.

________________________________________________________________________________________________________________________________
```
gcs_output_path = [
    "/home/airflow/gcs/data/raw_data/product.parquet",
    "/home/airflow/gcs/data/raw_data/customer.parquet",
    "/home/airflow/gcs/data/raw_data/transaction.parquet",
    "/home/airflow/gcs/data/raw_data/conversion_rate.parquet",
    "/home/airflow/gcs/data/cleaned.parquet"
]
```
:arrow_up: Next, I defined `GCS path` where to store all collected data and cleaned data.

________________________________________________________________________________________________________________________________
```
default_args = {
    'owner': 'teengai',
}
```
:arrow_up: Lastly, I defined `default arguments` as it is required when set up a DAG.

________________________________________________________________________________________________________________________________
## Set up Task to Get Data from MySQL Database
```
@task()
def get_data_from_mysql(table, output_path):
    # Call MySqlHook to connect to MySQL from the connection created in Airflow
    mysqlserver = MySqlHook()

    # Get Pandas DataFrame by Querying from Database using Hook
    df = mysqlserver.get_pandas_df(sql=f"SELECT * FROM r2de3.{table}")
    
    # Save as a PARQUET File and Output to GCS
    df.to_parquet(output_path, index=False)
```
- Using `@task()` to set up a task for this pipeline
- Create function called `get_data_from_mysql()` to get data from MySQL Database with table and output_path as a parameters
- Using `MySqlHook()` to connect to MySQL Databse through a connection created in Airflow
- Using `get_pandas_df()` to execute the sql and return a Pandas DataFrame
- Save as a parquet File and output to GCS without indexes

> ***Reference:** https://airflow.apache.org/docs/apache-airflow-providers-mysql/1.0.0/_api/airflow/providers/mysql/hooks/mysql/index.html*

> ***Reference:** https://airflow.apache.org/docs/apache-airflow/1.10.14/_api/airflow/hooks/dbapi_hook/index.html#airflow.hooks.dbapi_hook.DbApiHook.get_pandas_df*

## Set up Task to Get Data from API
```
@task()
def get_data_from_api(output_path):
    r = requests.get(CONVERSION_RATE_URL)
    result = r.json()
    df = pd.DataFrame(result)
    
    # Convert date column to date-data-type, Drop id column
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop(columns=["id"], axis=1)

    # Output to GCS
    df.to_parquet(output_path, index=False)
```
- Create funciton called `get_data_from_api()` to get data from API and save as Pandas DataFrame
- Due to the `date` column is in `STRING`, so, I converted it to a date data type using command `pd.to_datetime()`
- Save as a parquet File and output to GCS without indexes

> ***Reference:** https://requests.readthedocs.io/en/latest/*

## Set up Task for Cleaning Data
```
@task()
def merge_all(gcs_output_path, output_path):
    product = pd.read_parquet(gcs_output_path[0])
    customer = pd.read_parquet(gcs_output_path[1])
    transaction = pd.read_parquet(gcs_output_path[2])
    conversion_rate = pd.read_parquet(gcs_output_path[3])

    # Merge All DataFrames as final_df DataFrame
    final_df = transaction.merge(product, how="left", on="ProductNo") \
                            .merge(customer, how="left", on="CustomerNo") \
                            .merge(conversion_rate, how="left", left_on="Date", right_on="date")
    
    # Drop Duplicates and Null Values
    final_df = final_df.drop_duplicates()
    final_df = final_df.dropna()

    # Clean CustomerNo
    final_df['CustomerNo'] = final_df['CustomerNo'].astype(str)
    final_df['CustomerNo'] = final_df['CustomerNo'].str[:5].astype(int)

    # Clean Country
    final_df['Country'] = final_df['Country'].apply(lambda x:'Thailand' if x == 'Unspecified' else x)

    # Calculate Total Price in THB
    final_df['Price'] = (final_df['Price'] * final_df['gbp_thb']).round(2)
    final_df['total_amount'] = (final_df['Price'] * final_df['Quantity']).round(2)

    # Drop Unused Columns
    final_df = final_df.drop(columns=['date', 'gbp_thb'], axis=1)

    # Rename Columns
    final_df = final_df.rename(
        columns={
            'TransactionNo':'transaction_id',
            'Date':'date',
            'ProductNo':'product_id',
            'Price':'price',
            'Quantity':'quantity',
            'CustomerNo':'customer_id',
            'ProductName':'product_name',
            'Country':'country',
            'Name':'customer_name'
        }
    )

    # Save as a PARQUET File
    final_df.to_parquet(output_path, index=False)
```
- I defined a function named `merged_all()` to merge all tables, `product` `customer` `transaction` `conversion_rate`, by using command `pd.read_parquet()` and `.merge()`
- After filtering a duplicates and null values in Cleaning section, we can drop all of them
- `CustomerNo` column is in `FLOAT`, so I cast it to `STRING` and extracted only the number before the decimal point, then cast type to `INTEGER`
- I found out that there is one country named `Unspecified`, so, I decided to change it to `Thailand` by using `lambda function`
- Find `total sales` in `THB` and round it to two decimal point
- Drop `date` `gbp_thb` columns
- Rename column into a proper format
- Save as a parquet File and output to GCS without indexes


## Instantiate a DAG
```
# Instantiate a DAG
@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1))
def pipeline():

    # Define Tasks
    t1 = get_data_from_mysql(table="product", output_path=gcs_output_path[0])
    t2 = get_data_from_mysql(table="customer", output_path=gcs_output_path[1])
    t3 = get_data_from_mysql(table="transaction", output_path=gcs_output_path[2])
    t4 = get_data_from_api(output_path=gcs_output_path[3])
    t5 = merge_all(gcs_output_path, output_path=gcs_output_path[-1])

    # Upload to BigQuery
    t6 = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket="us-central1-r2de3-project-a-478426a4-bucket",
        source_objects=["data/cleaned.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="r2de3_project.transaction",
        write_disposition="WRITE_TRUNCATE"
    )

    # Set-up Dependencies
    [t1, t2, t3, t4] >> t5 >> t6

pipeline()
```

- Using `@dag()` to wrap a function into an `Airflow DAG` with paremeters, `default argument` `schedule_interval` `start_date`
- Create a function called `pipeline()` contains all tasks
- Applying each function created earlier as a specific variable, a task in sequence, in order to make it easily understandable
- Create new task, `t6`, using `GCSToBigQueryOperator()` to upload cleaned data from GCS to BigQuery
- Set up all tasks in sequence

***Reference:** https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html*

***Reference:** https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/decorators/index.html*

***Reference:** https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/transfers/gcs_to_bigquery/index.html*
