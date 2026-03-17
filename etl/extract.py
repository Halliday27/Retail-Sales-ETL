from __future__ import annotations

import io
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _read_local_csv(path: Path) -> pd.DataFrame:
    logger.info("Reading local file: %s", path)
    return pd.read_csv(path)


def _read_s3_csv(bucket: str, key: str) -> pd.DataFrame:
    logger.info("Reading S3 file: s3://%s/%s", bucket, key)
    import boto3

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def extract_data(source_mode: str, local_data_dir: str, s3_bucket: str, s3_prefix: str):
    if source_mode not in {"local", "s3"}:
        raise ValueError("source_mode must be 'local' or 's3'.")

    if source_mode == "local":
        base = Path(local_data_dir)
        customers = _read_local_csv(base / "customers.csv")
        products = _read_local_csv(base / "products.csv")
        orders = _read_local_csv(base / "orders.csv")
    else:
        customers = _read_s3_csv(s3_bucket, f"{s3_prefix}/customers/customers.csv")
        products = _read_s3_csv(s3_bucket, f"{s3_prefix}/products/products.csv")
        orders = _read_s3_csv(s3_bucket, f"{s3_prefix}/orders/orders.csv")

    logger.info(
        "Extracted rows | customers=%s products=%s orders=%s",
        len(customers),
        len(products),
        len(orders),
    )
    return customers, products, orders
