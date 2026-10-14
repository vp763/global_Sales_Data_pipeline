import os
import json
import datetime
import requests
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from airflow import DAG
from sqlalchemy import create_engine

from airflow.operators.python import PythonOperator

PROJECT_NAME = "global_sales"
BASE_DATA_DIR = f"/opt/airflow/data/{PROJECT_NAME}"

BRONZE_DIR = os.path.join(BASE_DATA_DIR, "bronze")
SILVER_DIR = os.path.join(BASE_DATA_DIR, "silver")
GOLD_DIR = os.path.join(BASE_DATA_DIR, "gold")

# Dhyan rakhna ki Mac par ye folder exist kare: ./data/global_sales/sourcecrm/customers_crm.csv
CUSTOMER_DATA = os.path.join(BASE_DATA_DIR, "sourcecrm", "customers_crm.csv")

# Folders auto create karne ke liye
for folder in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    os.makedirs(folder, exist_ok=True)


def extract_postgre(**kwargs):
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    df = pd.read_sql("SELECT * FROM customer_orders", engine)
    output_path = os.path.join(BRONZE_DIR, "orders_raw.json")
    df.to_json(output_path, orient="records", indent=4)
    print(f"Extracted {len(df)} orders from Postgres.")


def extract_forex_rate(**kwargs):
    response = requests.get('https://open.er-api.com/v6/latest/USD', timeout=30)
    response.raise_for_status()
    forex_data = response.json()

    output_path = os.path.join(BRONZE_DIR, "forex_rate.json")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(forex_data, f, indent=4)
    print(f"Forex rates successfully fetched.")


def extract_customer(**kwargs):
    df = pd.read_csv(CUSTOMER_DATA)
    output_path = os.path.join(BRONZE_DIR, "customers_crm.csv")
    df.to_csv(output_path, index=False)
    print(f"Staged {len(df)} customers from CRM.")




from pyspark.sql.functions import col, to_timestamp

def clean_orders_silver(**kwargs):
    spark = SparkSession.builder.appName("CleanOrders").getOrCreate()
    
    input_path = os.path.join(BRONZE_DIR, "orders_raw.json")
    df = spark.read.option("multiline", "true").json(input_path)
    
    # Typing aur status filter (REFUNDED hatana)
    df_clean = df.filter(col("order_status") != "REFUNDED") \
                 .withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
                 .withColumn("order_amount", col("order_amount").cast("double"))
    
    # Parquet format me Silver zone me save karo
    output_path = os.path.join(SILVER_DIR, "orders.parquet")
    df_clean.write.mode("overwrite").parquet(output_path)
    
    spark.stop()


from pyspark.sql.functions import when

def clean_customers_silver(**kwargs):
    spark = SparkSession.builder.appName("CleanCustomers").getOrCreate()
    
    input_path = os.path.join(BRONZE_DIR, "customers_crm.csv")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    
    # Deduplicate on customer_id
    df_dedup = df.dropDuplicates(["customer_id"])
    
    # Risk score validation: 0 to 1 ke beech ho, warna null
    df_clean = df_dedup.withColumn(
        "risk_score",
        when((col("risk_score") >= 0) & (col("risk_score") <= 1), col("risk_score").cast("double"))
        .otherwise(None)
    )
    
    output_path = os.path.join(SILVER_DIR, "customers.parquet")
    df_clean.write.mode("overwrite").parquet(output_path)
    
    spark.stop()


def clean_forex_silver(**kwargs):
    spark = SparkSession.builder.appName("CleanForex").getOrCreate()
    
    input_path = os.path.join(BRONZE_DIR, "forex_rate.json")
    # API JSON read karo aur key-value explode / parse karke do columns banao: currency, rate
    raw_df = spark.read.option("multiline", "true").json(input_path)
    
    # Forex rates ko tabular format me convert karo
    # (Ya tum chaho toh API dictionary ko parse karke seedha Spark DataFrame bana sakte ho)
    with open(input_path, 'r', encoding='utf-8') as f:
        forex_data = json.load(f)
    rates_list = [(k, float(v)) for k, v in forex_data.get("rates", {}).items()]
    
    df = spark.createDataFrame(rates_list, ["currency", "exchange_rate_to_usd"])
    
    output_path = os.path.join(SILVER_DIR, "forex_rates.parquet")
    df.write.mode("overwrite").parquet(output_path)
    
    spark.stop()


from pyspark.sql.functions import col, round

def create_gold_star_schema(**kwargs):
    spark = SparkSession.builder.appName("CreateGoldStarSchema").getOrCreate()
    
    # 1. Silver zone se data read karo
    orders_df = spark.read.parquet(os.path.join(SILVER_DIR, "orders.parquet"))
    customers_df = spark.read.parquet(os.path.join(SILVER_DIR, "customers.parquet"))
    forex_df = spark.read.parquet(os.path.join(SILVER_DIR, "forex_rates.parquet"))
    
    # 2. Currency conversion (Left join on 'currency')
    orders_with_forex_df = orders_df.join(forex_df, on="currency", how="left")
    
    # Amount ko USD me divide karke 2 decimal places tak round karo
    orders_with_forex_df = orders_with_forex_df.withColumn(
        "order_amount_usd",
        round(col("order_amount") / col("exchange_rate_to_usd"), 2)
    )
    
    # 3. Fact Table (Only keys + metrics)
    fact_sales_df = orders_with_forex_df.select(
        "order_id",
        "customer_id",
        "product_id",
        "order_timestamp",
        "quantity",
        "order_amount",
        "currency",
        "exchange_rate_to_usd",
        "order_amount_usd"
    )
    
    # 4. Dimension Table (Descriptive metadata)
    dim_customer_df = customers_df.select(
        "customer_id",
        "customer_name",
        "country",
        "customer_tier",
        "signup_date",
        "risk_score"
    )
    
    # 5. Gold Zone me Parquet write karo
    fact_sales_df.write.mode("overwrite").parquet(os.path.join(GOLD_DIR, "fact_sales.parquet"))
    dim_customer_df.write.mode("overwrite").parquet(os.path.join(GOLD_DIR, "dim_customer.parquet"))
    
    print("Gold Star Schema created successfully!")
    spark.stop()


with DAG(
    dag_id="global_sales_ingestion",
    start_date=datetime.datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["global_sales", "ingestion", "etl"]

) as dag:
    extract_postgre_task = PythonOperator(
        task_id="extract_postgre",
        python_callable=extract_postgre,
    )

    extract_forex_rate_task = PythonOperator(
        task_id="extract_forex_rate",
        python_callable=extract_forex_rate,
    )

    extract_customer_task = PythonOperator(
        task_id="extract_customer",
        python_callable=extract_customer,
    )
    clean_orders_silver_task = PythonOperator(
        task_id="clean_orders_silver",
        python_callable=clean_orders_silver,
    )
    clean_customers_silver_task = PythonOperator(
        task_id="clean_customers_silver",
        python_callable=clean_customers_silver,
    )
    clean_forex_silver_task = PythonOperator(
        task_id="clean_forex_silver",
        python_callable=clean_forex_silver,
    )
    create_gold_star_schema_task = PythonOperator(
        task_id="create_gold_star_schema",
        python_callable=create_gold_star_schema,
    )

    # Task dependencies
    [extract_postgre_task, extract_forex_rate_task, extract_customer_task] >> clean_orders_silver_task >> clean_customers_silver_task>> clean_forex_silver_task >> create_gold_star_schema_task 