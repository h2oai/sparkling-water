import os
import pytest
from pyspark.sql import SparkSession
from pysparkling.conf import H2OConf
from pysparkling.context import H2OContext
from pyspark.sql.types import StructType, StructField, DoubleType

from tests import unit_test_utils


@pytest.fixture(scope="module")
def spark(spark_conf):
    conf = unit_test_utils.get_default_spark_conf(spark_conf)
    return SparkSession.builder.config(conf=conf).getOrCreate()


@pytest.fixture(scope="module", autouse=True)
def hc(spark):
    return H2OContext.getOrCreate(H2OConf().setClusterSize(1))


@pytest.fixture(scope="module")
def prostateDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/prostate/prostate.csv")


@pytest.fixture(scope="module")
def loanDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/loan.csv")


@pytest.fixture(scope="module")
def insuranceDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/insurance.csv")


@pytest.fixture(scope="module")
def semiconductorDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/semiconductor.csv")


@pytest.fixture(scope="module")
def irisDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/iris/iris_wheader.csv")


@pytest.fixture(scope="module")
def airlinesDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/airlines/allyears2k_headers.csv")


@pytest.fixture(scope="module")
def carsDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/cars_20mpg.csv")


@pytest.fixture(scope="module")
def arrestsDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/USArrests.csv")


@pytest.fixture(scope="module")
def birdsDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/birds.csv")


@pytest.fixture(scope="module")
def prostateDataset(spark, prostateDatasetPath):
    return spark.read.csv(prostateDatasetPath, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def airlinesDataset(spark, airlinesDatasetPath):
    return spark.read.csv(airlinesDatasetPath, header=True, inferSchema=True)

@pytest.fixture(scope="module")
def semiconductorDataset(spark, semiconductorDatasetPath):
    return spark.read.csv(semiconductorDatasetPath, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def arrestsDataset(spark, arrestsDatasetPath):
    return spark.read.csv(arrestsDatasetPath, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def birdsDataset(spark, birdsDatasetPath):
    return spark.read.csv(birdsDatasetPath, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def craiglistDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/craigslistJobTitles.csv")


@pytest.fixture(scope="module")
def craiglistDataset(spark, craiglistDatasetPath):
    return spark.read.csv(craiglistDatasetPath, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def heartDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/coxph_test/heart.csv")


@pytest.fixture(scope="module")
def heartDataset(spark, heartDatasetPath):
    return spark.read.csv(heartDatasetPath, header=True, inferSchema=True)

@pytest.fixture(scope="module")
def discourdDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/anomaly/ecg_discord_test.csv")

@pytest.fixture(scope="module")
def discourdDataset(spark, discourdDatasetPath):
    numbers = range(1, 211)
    fields = list(map(lambda x: StructField("C" + str(x), DoubleType(), False), numbers))
    return spark.read.csv(discourdDatasetPath, header=False, schema=StructType(fields))
