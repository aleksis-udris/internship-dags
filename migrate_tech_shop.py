import json
import os
from datetime import datetime
import clickhouse_connect
from airflow.sdk import dag, task
from dotenv import load_dotenv


@dag(
    dag_id="migration",
    description="A simple json dataset reader DAG",
    catchup=False,
    tags=["json_reader", "table_update"]
)
def migration():
    @task()
    def data_extraction():
        base_path= "/home/aleks/airflow/dags/files/migration_files"
        customer_path= base_path + "/customers.json"
        brand_path= base_path + "/brands.json"
        item_path= base_path + "/items.json"
        transaction_path= base_path + "/transactions.json"
        tag_path= base_path + "/tags.json"
        item_to_tag_path= base_path + "/connections_tti.json"

        with open(customer_path) as file:
            customers = json.load(file)

        with open(brand_path) as file:
            brands = json.load(file)

        with open(item_path) as file:
            items = json.load(file)

        with open(transaction_path) as file:
            transactions = json.load(file)

        with open(tag_path) as file:
            tags = json.load(file)

        with open(item_to_tag_path) as file:
            tti = json.load(file)

        return {
            'customers': customers,
            'brands': brands,
            'items': items,
            'transactions': transactions,
            'tags': tags,
            'tti': tti
        }

    @task()
    def setup_tables():
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        client.query(
            "CREATE TABLE IF NOT EXISTS tech_shop_db.customers"
            "(customer_id UInt32, first_name LowCardinality(String), last_name LowCardinality(String))"
            "ENGINE = MergeTree()"
            "PRIMARY KEY customer_id"
        )

        client.query(
            "CREATE TABLE IF NOT EXISTS tech_shop_db.brands"
            "(brand_id UInt32, name String)"
            "ENGINE = MergeTree()"
            "PRIMARY KEY brand_id"
        )

        client.query(
            "CREATE TABLE IF NOT EXISTS tech_shop_db.items"
            "(item_id UInt32, name String, price Float32, count UInt32, brand_id UInt32)"
            "ENGINE = MergeTree()"
            "PRIMARY KEY item_id"
        )

        client.query(
            "CREATE TABLE IF NOT EXISTS tech_shop_db.transactions"
            "(transaction_id UInt32, date Date32, price_paid Float32, item_id UInt32, customer_id UInt32)"
            "ENGINE = MergeTree()"
            "PRIMARY KEY (transaction_id, date)"
        )

        client.query(
            "CREATE TABLE IF NOT EXISTS tech_shop_db.tags"
            "(tag_id UInt32, name String)"
            "ENGINE = MergeTree()"
            "PRIMARY KEY tag_id"
        )

        client.query(
            "CREATE TABLE IF NOT EXISTS tech_shop_db.tag_to_item"
            "(tag_id UInt32, item_id UInt32)"
            "ENGINE = MergeTree()"
            "PRIMARY KEY tag_id"
        )

        if client:
            client.close()

    @task()
    def populate_customers(data: dict):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        for item in data['customers']:
            client.insert(
                'tech_shop_db.customers',
                [[item['id'], item['first_name'], item['last_name']]],
                column_names=['customer_id', 'first_name', 'last_name']
            )
            print(f'Successfully inserted record for customer {item["id"]}')

        if client:
            client.close()

    @task()
    def populate_brands(data: dict):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        for item in data['brands']:
            client.insert(
                'tech_shop_db.brands',
                [[item['id'], item['name']]],
                column_names=['brand_id', 'name']
            )
            print(f'Successfully inserted record for brand {item["id"]}')

        if client:
            client.close()

    @task()
    def populate_items(data: dict):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        for item in data['items']:
            client.insert(
                'tech_shop_db.items',
                [[item['id'], item['name'], item['price'], item['count'], item['brand_id']]],
                column_names=['item_id', 'name', 'price', 'count', 'brand_id']
            )

            print(f'Successfully inserted row for item {item["id"]}')

        if client:
            client.close()

    @task()
    def populate_transactions(data: dict):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        for item in data['transactions']:
            client.insert(
                'tech_shop_db.transactions',
                [[item['id'], datetime.strptime(item['date'], '%Y-%m-%d').date(), item['price_paid'], item['item_id'],
                  item['customer_id']]],
                column_names=['transaction_id', 'date', 'price_paid', 'item_id', 'customer_id']
            )

            print(f'Successfully inserted row for transaction {item["id"]}')

        if client:
            client.close()

    @task()
    def populate_tags(data: dict):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        for item in data['tags']:
            client.insert(
                'tech_shop_db.tags',
                [[item['id'], item['name']]],
                column_names=['tag_id', 'name']
            )

            print(f'Successfully inserted a row for tag {item["id"]}')

        if client:
            client.close()

    @task()
    def populate_tti(data: dict):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        i = 0

        for item in data['tti']:
            client.insert(
                'tech_shop_db.tag_to_item',
                [[item['tag_id'], item['item_id']]],
                column_names=['tag_id', 'item_id']
            )

            print(f'Successfully inserted a row for tag {i}')

            i += 1

        if client:
            client.close()

    setup_tables() >> [populate_customers(data_extraction()), populate_brands(data_extraction()), populate_items(data_extraction()), populate_transactions(data_extraction()), populate_tags(data_extraction()), populate_tti(data_extraction())]

load_dotenv()
migration()
