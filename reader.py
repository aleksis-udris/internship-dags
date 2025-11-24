import json
from datetime import datetime
import clickhouse_connect
from airflow.sdk import dag, task
from dotenv import load_dotenv, dotenv_values
import os

load_dotenv()

@dag(
    dag_id="reader",
    description="A simple json dataset reader DAG",
    catchup=False,
    tags=["json_reader", "table_update"]
)
def json_reader():

    @task()
    def extract_data():
        data_path = "/home/aleks/airflow/dags/files/dataset.json"

        with open(data_path) as file:
            data = json.load(file)

        return data

    @task()
    def total_income(data: dict):

        total_income = 0

        for object in data:
            total_income += object['price']

        print(f'Total earnings: {total_income:.2f}')

    @task()
    def upload_to_clickhouse(data: dict):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        client.query(
            "CREATE TABLE IF NOT EXISTS airflow_schema.purchases "
            "(client String, item String, amount UInt32, price Float32, date Date)"
            "ENGINE = MergeTree() "
            "PRIMARY KEY client"
        )

        for item in data:
            client.insert(
                'airflow_schema.purchases',
                [[item['client'], item['item'], item['amount'], item['price'], datetime.strptime(item['date'], '%Y-%m-%d').date()]],
                column_names=['client', 'item', 'amount', 'price', 'date']
            )
            print(f'Successfully inserted record for {item["client"]}')

        if client:
            client.close()

    dataset = extract_data()
    total_income(dataset)
    upload_to_clickhouse(dataset)

load_dotenv()
json_reader()