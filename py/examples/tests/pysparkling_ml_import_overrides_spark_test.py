from pyspark.sql import SparkSession
from pysparkling.ml import *
spark = (SparkSession.builder
         .appName("test")
         .enableHiveSupport()
         .getOrCreate()
         )

sc = spark.sparkContext

spark.sql("create table if not exists test_table (id INT)")

# Check that the table was correctly created
assert len(spark.sql("show tables").filter("tableName == 'test_table'").collect()) == 1