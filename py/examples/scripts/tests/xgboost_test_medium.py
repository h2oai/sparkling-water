from pyspark.sql import SparkSession
from pysparkling import *
import h2o
from h2o.estimators.xgboost import *
import os

spark = (SparkSession.builder
         .appName("test")
         .enableHiveSupport()
         .getOrCreate()
         )


# This is just helper function returning path to data-files
def _locate(filename):
    if os.path.isfile("/home/0xdiag/bigdata/laptop/milsongs/" + filename):
        return "/home/0xdiag/bigdata/laptop/milsongs/" + filename
    else:
        return "/home/0xdiag/bigdata/laptop/milsongs/" + filename


hc = H2OContext.getOrCreate()

training_frame = h2o.import_file(_locate("milsongs-train.csv.gz"))
test_frame = h2o.import_file(_locate("milsongs-test.csv.gz"))
x = list(range(1,training_frame.ncol))
y = 0

# Model with maximum of 2 trees
model_2_trees = H2OXGBoostEstimator(training_frame=training_frame, learn_rate=0.3,
                                    booster='gbtree', seed=1, ntrees=2, distribution='gaussian')
model_2_trees.train(x=x, y=y, training_frame=training_frame)
prediction_2_trees = model_2_trees.predict(test_frame)

assert prediction_2_trees.nrows == test_frame.nrows

# Model with 10 trees
model_10_trees = H2OXGBoostEstimator(training_frame=training_frame, learn_rate=0.3,
                                     booster='gbtree', seed=1, ntrees=10, distribution='gaussian')
model_10_trees.train(x=x, y=y, training_frame=training_frame)
prediction_10_trees = model_10_trees.predict(test_frame)

assert prediction_10_trees.nrows == test_frame.nrows

## Mean square error on model with lower number of decision trees should be higher
assert model_2_trees.mse() > model_10_trees.mse()