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
        "products_tags": ["product_id", "tag_id"],
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
    def extract_clickhouse(table_name, columns):
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True
        )
        # Use explicit column list instead of SELECT *
        col_str = ", ".join(columns)
        result = client.query(f"SELECT {col_str} FROM SHOP_DATA_LAKE.{table_name}")
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

        # Build sets of primary keys for comparison
        ch_keys = {row[0] for row in ch_rows}
        pg_keys = {row[0] for row in pg_rows}
        pg_map = {row[0]: row for row in pg_rows}

        to_insert = []
        to_delete = []

        # DETECT INSERTS - rows in PG but not in CH
        for key in pg_keys:
            if key not in ch_keys:
                to_insert.append(pg_map[key])

        # DETECT DELETES - rows in CH but not in PG
        for key in ch_keys:
            if key not in pg_keys:
                to_delete.append(key)

        print(f"[{table_name}] Summary: insert={len(to_insert)}, delete={len(to_delete)}")
        print(f"[{table_name}] PG keys: {pg_keys}")
        print(f"[{table_name}] CH keys: {ch_keys}")

        # DELETE rows that don't exist in PostgreSQL
        if to_delete:
            key_list = ",".join(str(k) for k in to_delete)
            client.command(f"ALTER TABLE SHOP_DATA_LAKE.{table_name} DELETE WHERE {pk} IN ({key_list})")
            client.command(f"OPTIMIZE TABLE SHOP_DATA_LAKE.{table_name} FINAL")

        # INSERT new rows from PostgreSQL
        if to_insert:
            client.insert(f"SHOP_DATA_LAKE.{table_name}", to_insert, column_names=columns)

        client.close()

        return {
            "inserted": len(to_insert),
            "deleted": len(to_delete)
        }

    for table_name, columns in TABLES_CONFIG.items():
        pg_data = extract_postgres(table_name, columns)
        ch_data = extract_clickhouse(table_name, columns)  # Pass columns here

        sync_table(
            table_name=table_name,
            columns=columns,
            pg_rows=pg_data,
            ch_rows=ch_data
        )


extract_and_load_dag = extract_and_load()