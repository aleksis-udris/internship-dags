import json
from datetime import datetime
import clickhouse_connect
from airflow.sdk import dag, task

import os
from dotenv import load_dotenv

@dag(
    dag_id="stock-reader",
    description="A simple json dataset reader DAG",
    catchup=False,
    tags=["json_reader", "table_update"]
)
def stock_tallier():

    @task()
    def extract_stock():
        data_path = "/home/aleks/airflow/dags/files/dataset-stock.json"

        with open(data_path) as file:
            data = json.load(file)

        return data

    @task()
    def total_value(data: dict):

        total_income = 0

        for object in data:
            total_income += object['price'] * object['stock']

        print(f'total stock value: {total_income:.2f}')

    @task()
    def upload_stock(data: dict):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        client.query(
            "CREATE TABLE IF NOT EXISTS airflow_schema.stock"
            "(name LowCardinality(String), description String, price Float32, stock UInt32)"
            "ENGINE = MergeTree() "
            "PRIMARY KEY name"
        )

        for item in data:
            client.insert(
                'airflow_schema.stock',
                [[item['name'], item['description'], item['price'], item['stock']]],
                column_names=['name', 'description', 'price', 'stock']
            )
            print(f'Successfully inserted record for {item["name"]}')

        if client:
            client.close()

    stock_data = extract_stock()
    total_value(stock_data)
    upload_stock(stock_data)

load_dotenv()
stock_tallier()
