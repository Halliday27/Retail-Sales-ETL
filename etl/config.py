from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    source_mode: str = os.getenv("SOURCE_MODE", "local")
    local_data_dir: str = os.getenv("LOCAL_DATA_DIR", "data")
    s3_bucket: str = os.getenv("S3_BUCKET", "")
    s3_prefix: str = os.getenv("S3_PREFIX", "raw")
    database_url: str = os.getenv("DATABASE_URL", "")


def get_config() -> AppConfig:
    return AppConfig()
