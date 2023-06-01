import h2o
import os
from pyspark.mllib.linalg import *
from pyspark.sql.types import *


def testSimpleParquetImport(spark):
    df = spark.sparkContext.parallelize([(num, "text") for num in range(0, 100)]).toDF().coalesce(1)
    df.write.mode('overwrite').parquet("file://" + os.path.abspath("build/tests_tmp/test.parquet"))

    parquetFile = None
    for file in os.listdir(os.path.abspath("build/tests_tmp/test.parquet")):
        if file.endswith(".parquet"):
            # it is always set
            parquetFile = file
    frame = h2o.upload_file(path=os.path.abspath("build/tests_tmp/test.parquet/" + parquetFile))
    assert frame.ncols == len(df.columns)
    assert frame.nrows == df.count()
    assert frame[0, 0] == 0.0
    assert frame[0, 1] == "text"


def testImportGCS():
    fr = h2o.import_file(
        "gs://gcp-public-data-nexrad-l2/2018/01/01/KABR/NWS_NEXRAD_NXL2DPBL_KABR_20180101050000_20180101055959.tar")
