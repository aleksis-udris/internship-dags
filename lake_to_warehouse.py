import os
from typing import Dict, List

import clickhouse_connect
from airflow.sdk import dag, task
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# DAG schedule and metadata
@dag(
    dag_id="sync_warehouse",
    schedule="@hourly",
    description="Sync ClickHouse Warehouse",
    start_date=pd.Timestamp("2023-01-01"),
    catchup=False,
    tags=["warehouse", "scd2", "clickhouse", "sync"],
)
def sync_data_warehouse():
    def ch_client():
        return clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST"),
            user=os.getenv("CLICKHOUSE_USER"),
            password=os.getenv("CLICKHOUSE_PASSWORD"),
            secure=True,
        )

    DIM_CONFIG: Dict[str, Dict] = {
        "dim_brands": {
            "lake_query": "SELECT brand_id, name AS brand_name FROM SHOP_DATA_LAKE.brands",
            "pk_columns": ["brand_id"],
            "dim_columns": ["brand_id", "brand_name"],
        },
        "dim_categories": {
            "lake_query": (
                "SELECT category_id, name AS category_name, supercategory_id "
                "FROM SHOP_DATA_LAKE.categories"
            ),
            "pk_columns": ["category_id"],
            "dim_columns": ["category_id", "category_name", "supercategory_id"],
        },
        "dim_products": {
            "lake_query": "SELECT product_id, name AS product_name FROM SHOP_DATA_LAKE.products",
            "pk_columns": ["product_id"],
            "dim_columns": ["product_id", "product_name"],
        },
        # dim_tags: join products_tags + tags to produce product_id, tag_id, tag_name
        "dim_tags": {
            "lake_query": (
                "SELECT pt.product_id, pt.tag_id, t.name AS tag_name "
                "FROM SHOP_DATA_LAKE.products_tags AS pt "
                "JOIN SHOP_DATA_LAKE.tags AS t USING (tag_id)"
            ),
            "pk_columns": ["product_id", "tag_id"],
            "dim_columns": ["product_id", "tag_id", "tag_name"],
        },
        "dim_users": {
            "lake_query": (
                "SELECT u.user_id, p.name, p.surname, u.email, u.password "
                "FROM SHOP_DATA_LAKE.users AS u "
                "LEFT JOIN SHOP_DATA_LAKE.profiles AS p "
                "    ON u.user_id = p.profile_id"
            ),
            "pk_columns": ["user_id"],
            "dim_columns": ["user_id", "name", "surname", "email", "password"],
        },
        "dim_warehouses": {
            "lake_query": (
                "SELECT warehouse_id, name AS warehouse_name FROM SHOP_DATA_LAKE.warehouses"
            ),
            "pk_columns": ["warehouse_id"],
            "dim_columns": ["warehouse_id", "warehouse_name"],
        }
    }

    @task()
    def sync_dimension(dim_name: str, cfg: Dict) -> str:
        lake_query = cfg["lake_query"]
        pk_columns: List[str] = cfg["pk_columns"]
        dim_columns: List[str] = cfg["dim_columns"]

        client = ch_client()

        cols_csv = ", ".join(dim_columns)
        pk_csv = ", ".join(pk_columns)

        non_pk_cols = [column for column in dim_columns if column not in pk_columns]
        if non_pk_cols:
            dl_non_pk_expr = " , ".join([f"dl.{column}" for column in non_pk_cols])
            dw_non_pk_expr = " , ".join([f"dw.{column}" for column in non_pk_cols])
            compare_expr = (
                f"CONCAT_WS(',', {dl_non_pk_expr}) != CONCAT_WS(',', {dw_non_pk_expr})"
            )
        else:
            compare_expr = "0 = 1"

        insert_sql = f"""
            INSERT INTO SHOP_DATA_WAREHOUSE.{dim_name} (
                {cols_csv},
                deleted,
                created_at,
                updated_at
            )
            SELECT
                {cols_csv},
                0 AS deleted,
                now() AS created_at,
                -- set updated_at only when there exists a previous live row and values differ
                IF(dw.{pk_columns[0]} IS NULL, NULL, IF({compare_expr}, now(), NULL)) AS updated_at
            FROM (
                {lake_query}
            ) AS dl
            LEFT JOIN (
                SELECT {cols_csv}
                FROM SHOP_DATA_WAREHOUSE.{dim_name}
                WHERE deleted = 0
            ) AS dw
            USING ({pk_csv})
            WHERE
                dw.{pk_columns[0]} IS NULL
                OR {compare_expr}
        """
        client.command(insert_sql)

        if len(pk_columns) == 1:
            not_in_clause = f"{pk_columns[0]} NOT IN (SELECT {pk_columns[0]} FROM ({lake_query}))"
        else:
            # tuple-based NOT IN
            pk_tuple = ", ".join(pk_columns)
            not_in_clause = f"({pk_tuple}) NOT IN (SELECT {pk_tuple} FROM ({lake_query}))"

        delete_sql = f"""
            ALTER TABLE SHOP_DATA_WAREHOUSE.{dim_name}
            UPDATE
                deleted = 1,
                deleted_at = now()
            WHERE
                {not_in_clause}
                AND deleted = 0
        """
        client.command(delete_sql)

        client.close()
        return f"{dim_name} synchronized"

    FACT_LAKE_QUERY = """
        SELECT
            t.transaction_id,
            t.user_id,
            t.product_id,
            t.warehouse_id,
            p.category_id,
            p.price,
            p.brand_id
        FROM SHOP_DATA_LAKE.transactions AS t
        JOIN SHOP_DATA_LAKE.products AS p USING (product_id)
    """

    @task()
    def sync_transaction_facts():
        client = ch_client()

        insert_sql = f"""
            INSERT INTO SHOP_DATA_WAREHOUSE.transaction_facts (
                transaction_id,
                user_id,
                product_id,
                warehouse_id,
                category_id,
                price,
                brand_id,
                deleted
            )
            SELECT
                fl.transaction_id,
                fl.user_id,
                fl.product_id,
                fl.warehouse_id,
                fl.category_id,
                fl.price,
                fl.brand_id,
                0 AS deleted
            FROM (
                {FACT_LAKE_QUERY}
            ) AS fl
            LEFT JOIN (
                SELECT
                    transaction_id,
                    user_id,
                    product_id,
                    warehouse_id,
                    category_id,
                    price,
                    brand_id
                FROM SHOP_DATA_WAREHOUSE.transaction_facts
                WHERE deleted = 0
            ) AS dw USING (transaction_id)
            WHERE
                dw.transaction_id IS NULL
                OR (
                    -- any differing attribute => new version
                    (fl.user_id != dw.user_id)
                    OR (fl.product_id != dw.product_id)
                    OR (fl.warehouse_id != dw.warehouse_id)
                    OR (fl.category_id != dw.category_id)
                    OR (fl.price != dw.price)
                    OR (fl.brand_id != dw.brand_id)
                )
        """
        client.command(insert_sql)

        delete_sql = f"""
            ALTER TABLE SHOP_DATA_WAREHOUSE.transaction_facts
            UPDATE
                deleted = 1
            WHERE
                transaction_id NOT IN (SELECT transaction_id FROM ({FACT_LAKE_QUERY}))
                AND deleted = 0
        """
        client.command(delete_sql)

        client.close()
        return "transaction_facts synchronized"

    dim_tasks = []
    for dim_name, cfg in DIM_CONFIG.items():
        task_callable = sync_dimension.override(task_id=f"sync_{dim_name}")
        dim_tasks.append(task_callable(dim_name, cfg))

    sync_facts_task = sync_transaction_facts()

    for t in dim_tasks:
        t >> sync_facts_task

    return "all sync tasks scheduled"

sync_data_warehouse = sync_data_warehouse()