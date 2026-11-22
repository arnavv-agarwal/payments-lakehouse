"""Shared test setup: the job modules on the import path, and one local Spark session.

The tests exercise the pure transformation logic of the jobs (no Kafka, no
Iceberg, no AWS), so they run anywhere with Java and pyspark installed: on a
laptop, in the Spark image, or in GitHub Actions.
"""
import os
import sys
import time
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "jobs"))

# PySpark converts timestamps to and from Python datetimes in the *process's*
# local time zone. Pin it to UTC (like the jobs' session time zone) so the
# expected values hold on any machine, including a laptop set to IST.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("lakehouse-tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
