import os

import clickhouse_connect
import psycopg2
import pandas as pd
from airflow.sdk import dag, task
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_NAME"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASS"),
        host=os.getenv("POSTGRES_HOST"),
        connect_timeout=10,
        sslmode="prefer",
        port=os.getenv("POSTGRES_PORT")
    )


@dag(
    dag_display_name="Shop Data Extractor and Loader",
    dag_id="data_shop_etl",
    description="Sync Postgres tables into ClickHouse",
    schedule="@hourly",
    start_date=pd.Timestamp("2023-01-01"),
    catchup=False,
    tags=["extract", "load", "data"]
)
def extract_and_load():

    TABLES_CONFIG = {
        "brands": ["brand_id", "name", "created_at", "updated_at"],
        "categories": ["category_id", "name", "supercategory_id", "created_at", "updated_at"],
        "products": ["product_id", "name", "price", "description", "warehouse_id", "brand_id", "category_id", "created_at", "updated_at"],
        "products_tags": ["product_id", "tag_id", "created_at", "updated_at"],
        "tags": ["tag_id", "name", "created_at", "updated_at"],
        "profiles": ["profile_id", "name", "surname", "created_at", "updated_at"],
        "transactions": ["transaction_id", "user_id", "product_id", "warehouse_id", "created_at", "updated_at"],
        "users": ["user_id", "email", "password", "created_at", "updated_at"],
        "warehouses": ["warehouse_id", "name", "created_at", "updated_at"]
    }

    @task()
    def extract_postgres(table_name, columns):
        conn = get_db_connection()
        cur = conn.cursor()
        col_str = ", ".join(columns)

        cur.execute(f"SELECT {col_str} FROM iManagement.{table_name}")
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return [list(r) for r in rows]

    @task()
    def extract_clickhouse(table_name):
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )
        result = client.query(f"SELECT * FROM SHOP_DATA_LAKE.{table_name}")
        client.close()

        return [list(r) for r in result.result_rows]

    @task()
    def sync_table(table_name, columns, pg_rows, ch_rows):

        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )

        pk = columns[0]

        ch_map = {row[0]: row for row in ch_rows}

        pg_map = {row[0]: row for row in pg_rows}

        to_insert = []
        to_update = []
        to_delete = []

        # DETECT INSERTS/UPDATES
        for key, pg_row in pg_map.items():
            if key not in ch_map:
                to_insert.append(pg_row)
            else:
                if pg_row != ch_map[key]:     # row changed
                    to_update.append(pg_row)

        # DETECT DELETES
        for key in ch_map.keys():
            if key not in pg_map:
                to_delete.append([key])

        #DELETE
        if to_delete:
            keys = [row[0] for row in to_delete]
            key_list = ",".join(str(k) for k in keys)
            client.command(f"ALTER TABLE SHOP_DATA_LAKE.{table_name} DELETE WHERE {pk} IN ({key_list})")

        for row in to_update:
            key = row[0]
            client.command(f"ALTER TABLE SHOP_DATA_LAKE.{table_name} DELETE WHERE {pk} = {key}")

        if to_update:
            client.insert(f"SHOP_DATA_LAKE.{table_name}", to_update, column_names=columns)

        if to_insert:
            client.insert(f"SHOP_DATA_LAKE.{table_name}", to_insert, column_names=columns)

        client.close()

        return {
            "inserted": len(to_insert),
            "updated": len(to_update),
            "deleted": len(to_delete)
        }

    for table_name, columns in TABLES_CONFIG.items():

        pg_data = extract_postgres(table_name, columns)
        ch_data = extract_clickhouse(table_name)

        sync_table(
            table_name=table_name,
            columns=columns,
            pg_rows=pg_data,
            ch_rows=ch_data
        )


extract_and_load_dag = extract_and_load()