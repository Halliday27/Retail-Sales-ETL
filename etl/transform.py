from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform_data(df_customers: pd.DataFrame, df_products: pd.DataFrame, df_orders: pd.DataFrame):
    logger.info("Starting data transformation")

    dim_customer = (
        df_customers.dropna(subset=["customer_id"]).drop_duplicates(subset=["customer_id"]).copy()
    )
    dim_customer["customer_name"] = dim_customer["customer_name"].astype(str).str.strip()
    dim_customer["city"] = dim_customer["city"].astype(str).str.title().str.strip()
    dim_customer["country"] = dim_customer["country"].astype(str).str.title().str.strip()

    dim_product = (
        df_products.dropna(subset=["product_id"]).drop_duplicates(subset=["product_id"]).copy()
    )
    dim_product["unit_price"] = pd.to_numeric(dim_product["unit_price"], errors="coerce")
    dim_product = dim_product.dropna(subset=["unit_price"])

    clean_orders = df_orders.copy()
    clean_orders["quantity"] = pd.to_numeric(clean_orders["quantity"], errors="coerce")
    clean_orders = clean_orders[clean_orders["quantity"] > 0]
    clean_orders["order_date"] = pd.to_datetime(clean_orders["order_date"], errors="coerce")
    clean_orders = clean_orders.dropna(subset=["order_id", "customer_id", "product_id", "order_date"])
    clean_orders = clean_orders.drop_duplicates(subset=["order_id"])

    dim_date = clean_orders[["order_date"]].drop_duplicates().copy()
    dim_date["full_date"] = dim_date["order_date"].dt.date
    dim_date["year"] = dim_date["order_date"].dt.year
    dim_date["month"] = dim_date["order_date"].dt.month
    dim_date["day"] = dim_date["order_date"].dt.day
    dim_date["date_key"] = dim_date["order_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date = dim_date[["date_key", "full_date", "year", "month", "day"]]

    fact_sales = clean_orders.merge(
        dim_product[["product_id", "unit_price"]], on="product_id", how="inner"
    )
    fact_sales["date_key"] = fact_sales["order_date"].dt.strftime("%Y%m%d").astype(int)
    fact_sales["total_amount"] = fact_sales["quantity"] * fact_sales["unit_price"]
    fact_sales = fact_sales[
        [
            "order_id",
            "date_key",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "total_amount",
        ]
    ]

    logger.info(
        "Transformed rows | dim_customer=%s dim_product=%s dim_date=%s fact_sales=%s",
        len(dim_customer),
        len(dim_product),
        len(dim_date),
        len(fact_sales),
    )
    return dim_customer, dim_product, dim_date, fact_sales
