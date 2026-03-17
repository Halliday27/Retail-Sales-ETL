from __future__ import annotations

import logging

from etl.config import get_config
from etl.extract import extract_data
from etl.load import load_data
from etl.transform import transform_data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


def run() -> None:
    cfg = get_config()
    df_customers, df_products, df_orders = extract_data(
        source_mode=cfg.source_mode,
        local_data_dir=cfg.local_data_dir,
        s3_bucket=cfg.s3_bucket,
        s3_prefix=cfg.s3_prefix,
    )
    dim_customer, dim_product, dim_date, fact_sales = transform_data(
        df_customers, df_products, df_orders
    )
    load_data(cfg.database_url, dim_customer, dim_product, dim_date, fact_sales)


if __name__ == "__main__":
    run()
