from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def _execute_sql_file(cursor, sql_file: str):
    sql_text = Path(sql_file).read_text(encoding="utf-8")
    cursor.execute(sql_text)


def _upsert_dim_customer(cur, df: pd.DataFrame):
    rows = [tuple(r) for r in df[["customer_id", "customer_name", "city", "country"]].to_numpy()]
    execute_values(
        cur,
        """
        INSERT INTO dim_customer (customer_id, customer_name, city, country)
        VALUES %s
        ON CONFLICT (customer_id)
        DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            city = EXCLUDED.city,
            country = EXCLUDED.country
        """,
        rows,
    )


def _upsert_dim_product(cur, df: pd.DataFrame):
    rows = [tuple(r) for r in df[["product_id", "product_name", "category", "unit_price"]].to_numpy()]
    execute_values(
        cur,
        """
        INSERT INTO dim_product (product_id, product_name, category, unit_price)
        VALUES %s
        ON CONFLICT (product_id)
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            unit_price = EXCLUDED.unit_price
        """,
        rows,
    )


def _upsert_dim_date(cur, df: pd.DataFrame):
    rows = [tuple(r) for r in df[["date_key", "full_date", "year", "month", "day"]].to_numpy()]
    execute_values(
        cur,
        """
        INSERT INTO dim_date (date_key, full_date, year, month, day)
        VALUES %s
        ON CONFLICT (date_key)
        DO UPDATE SET
            full_date = EXCLUDED.full_date,
            year = EXCLUDED.year,
            month = EXCLUDED.month,
            day = EXCLUDED.day
        """,
        rows,
    )


def _build_fact_rows(cur, fact_sales: pd.DataFrame):
    cur.execute("SELECT customer_id, customer_key FROM dim_customer")
    customer_lookup = dict(cur.fetchall())
    cur.execute("SELECT product_id, product_key FROM dim_product")
    product_lookup = dict(cur.fetchall())

    rows = []
    for rec in fact_sales.to_dict(orient="records"):
        ck = customer_lookup.get(int(rec["customer_id"]))
        pk = product_lookup.get(int(rec["product_id"]))
        if ck is None or pk is None:
            continue
        rows.append(
            (
                int(rec["order_id"]),
                int(rec["date_key"]),
                ck,
                pk,
                int(rec["quantity"]),
                float(rec["unit_price"]),
                float(rec["total_amount"]),
            )
        )
    return rows


def _upsert_fact_sales(cur, rows):
    if not rows:
        logger.warning("No fact rows to load")
        return
    execute_values(
        cur,
        """
        INSERT INTO fact_sales
            (order_id, date_key, customer_key, product_key, quantity, unit_price, total_amount)
        VALUES %s
        ON CONFLICT (order_id)
        DO UPDATE SET
            date_key = EXCLUDED.date_key,
            customer_key = EXCLUDED.customer_key,
            product_key = EXCLUDED.product_key,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            total_amount = EXCLUDED.total_amount
        """,
        rows,
    )


def load_data(database_url: str, dim_customer: pd.DataFrame, dim_product: pd.DataFrame, dim_date: pd.DataFrame, fact_sales: pd.DataFrame):
    if not database_url:
        raise ValueError("DATABASE_URL is required.")

    logger.info("Loading data to PostgreSQL")
    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            _execute_sql_file(cur, "sql/create_tables.sql")
            _upsert_dim_customer(cur, dim_customer)
            _upsert_dim_product(cur, dim_product)
            _upsert_dim_date(cur, dim_date)
            fact_rows = _build_fact_rows(cur, fact_sales)
            _upsert_fact_sales(cur, fact_rows)

    logger.info("Load completed successfully")
