from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("m10-dq-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled",             "false")
        .getOrCreate()
    )
