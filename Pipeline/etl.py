from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import requests
import pandas as pd


# API URL
CONVERSION_RATE_URL = "https://r2de3-currency-api-vmftiryt6q-as.a.run.app/gbp_thb"


# Path
gcs_output_path = [
    "/home/airflow/gcs/data/raw_data/product.parquet",
    "/home/airflow/gcs/data/raw_data/customer.parquet",
    "/home/airflow/gcs/data/raw_data/transaction.parquet",
    "/home/airflow/gcs/data/raw_data/conversion_rate.parquet",
    "/home/airflow/gcs/data/cleaned.parquet"
]


# Default Args
default_args = {
    'owner': 'teengai',
}


# Get Data from MySQL
@task()
def get_data_from_mysql(table, output_path):
    # Call MySqlHook to connect to MySQL from the connection created in Airflow
    mysqlserver = MySqlHook()

    # Get Pandas DataFrame by Querying from Database using Hook
    df = mysqlserver.get_pandas_df(sql=f"SELECT * FROM r2de3.{table}")
    
    # Save as a PARQUET File and Output to GCS
    df.to_parquet(output_path, index=False)


# Get Data from API
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


# Merge All DataFrames
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