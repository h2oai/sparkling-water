import pytest
from pyspark.sql import SparkSession
from tests import unit_test_utils


@pytest.fixture(scope="module")
def spark(spark_conf):
    conf = unit_test_utils.get_default_spark_conf(spark_conf)
    return SparkSession.builder.config(conf=conf).getOrCreate()
