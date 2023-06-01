import pytest

from tests.integration import integ_test_utils


@pytest.fixture(scope="module")
def integ_spark_conf(spark_conf, dist):
    spark_conf["spark.master"] = "local-cluster[2,1,2048]"
    spark_conf["spark.submit.pyFiles"] = dist
    return integ_test_utils.get_default_spark_conf(spark_conf)
