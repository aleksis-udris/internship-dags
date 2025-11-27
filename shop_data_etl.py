import os

import clickhouse_connect
import psycopg2
import pandas as pd
from airflow.sdk import dag, task
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(dbname=os.getenv("POSTGRES_NAME"),
                            user=os.getenv("POSTGRES_USER"),
                            password=os.getenv("POSTGRES_PASS"),
                            host=os.getenv("POSTGRES_HOST"),
                            connect_timeout=10,
                            sslmode="prefer",
                            port=os.getenv("POSTGRES_PORT"))

@dag(
    dag_display_name="Shop Data Extractor and Loader",
    dag_id="data_shop_etl",
    description="A simple json dataset reader DAG",
    schedule="@hourly",
    start_date=pd.Timestamp("2023-01-01"),
    catchup=False,
    tags=["extract", "load", "data"]
)
def extract_and_load():
    TABLES_CONFIG = {
        "brands": ["brand_id", "name"],
        "categories": ["category_id", "name", "supercategory_id"],
        "products": ["product_id", "name", "price", "description", "warehouse_id", "brand_id", "category_id"],
        "products_tags": ["product_id", "tag_id"],
        "tags": ["tag_id", "name"],
        "profiles": ["profile_id", "name", "surname"],
        "transactions": ["transaction_id", "user_id", "product_id", "warehouse_id"],
        "users": ["user_id", "email", "password"],
        "warehouses": ["warehouse_id", "name"]
    }

    @task()
    def extract_table(table_name):
        conn = get_db_connection()
        cur = conn.cursor()
        query = f"SELECT * FROM iManagement.{table_name}"
        cur.execute(query)
        data = cur.fetchall()
        cur.close()
        conn.close()
        print(f"Extracted {len(data)} rows from {table_name}")
        return data

    @task()
    def create_tables():
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        client.query("CREATE DATABASE IF NOT EXISTS SHOP_DATA_LAKE")

        client.query("CREATE TABLE IF NOT EXISTS SHOP_DATA_LAKE.brands"
                     "(brand_id UInt32, name LowCardinality(String))"
                     " ENGINE = MergeTree()" 
                     "PRIMARY KEY brand_id")

        client.query("CREATE TABLE IF NOT EXISTS SHOP_DATA_LAKE.categories"
                     "(category_id UInt32, name LowCardinality(String), supercategory_id Nullable(UInt32))"
                     " ENGINE = MergeTree()" 
                     "PRIMARY KEY category_id")

        client.query("CREATE TABLE IF NOT EXISTS SHOP_DATA_LAKE.products"
                     "(product_id UInt32, name LowCardinality(String), price Float32, description String, warehouse_id UInt32, brand_id UInt32, category_id UInt32)"
                     " ENGINE = MergeTree()" 
                     "PRIMARY KEY product_id")

        client.query("CREATE TABLE IF NOT EXISTS SHOP_DATA_LAKE.products_tags"
                     "(product_id UInt32, tag_id UInt32)"
                     " ENGINE = MergeTree()"
                     "PRIMARY KEY (product_id, tag_id)")

        client.query("CREATE TABLE IF NOT EXISTS SHOP_DATA_LAKE.tags"
                     "(tag_id UInt32, name LowCardinality(String))"
                     " ENGINE = MergeTree()" 
                     "PRIMARY KEY tag_id")

        client.query("CREATE TABLE IF NOT EXISTS SHOP_DATA_LAKE.profiles"
                     "(profile_id UInt32, name LowCardinality(String), surname LowCardinality(String))"
                     " ENGINE = MergeTree()" 
                     "PRIMARY KEY profile_id")

        client.query("CREATE TABLE IF NOT EXISTS SHOP_DATA_LAKE.transactions"
                     "(transaction_id UInt32, user_id UInt32, product_id UInt32, warehouse_id UInt32)"
                     " ENGINE = MergeTree()" 
                     "PRIMARY KEY (transaction_id, user_id)")

        client.query("CREATE TABLE IF NOT EXISTS SHOP_DATA_LAKE.users"
                     "(user_id UInt32, email LowCardinality(String), password LowCardinality(String))"
                     " ENGINE = MergeTree()" 
                     "PRIMARY KEY user_id")

        client.query("CREATE TABLE IF NOT EXISTS SHOP_DATA_LAKE.warehouses"
                     "(warehouse_id UInt32, name LowCardinality(String))"
                     " ENGINE = MergeTree()" 
                     "PRIMARY KEY warehouse_id")

        client.close()


    @task()
    def load_table(data, table_name, columns):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        client.insert(f"SHOP_DATA_LAKE.{table_name}", data, column_names=columns)

        client.close()

    create = create_tables()
    load_tasks = []
    for table_name, columns in TABLES_CONFIG.items():
        extracted = extract_table(table_name)
        loaded = load_table(extracted, table_name, columns)
        create >> extracted >> loaded  # chain: create_tables -> extract -> load
        load_tasks.append(loaded)

extract_and_load_dag = extract_and_load()