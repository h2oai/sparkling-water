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
def _locate_prostate_csv():
    if os.path.isfile("/home/0xdiag/smalldata/prostate/prostate.csv"):
        return "/home/0xdiag/smalldata/prostate/prostate.csv"
    else:
        return "../examples/smalldata/prostate/prostate.csv"


hc = H2OContext.getOrCreate(spark)

prostate_frame = h2o.import_file(_locate_prostate_csv())
x = ['RACE']
y = 'CAPSULE'
prostate_frame[y] = prostate_frame[y].asfactor()

prostate_frame.split_frame(ratios=[0.75], destination_frames=['prostate_training', 'prostate_validation'], seed=1)

training_frame = h2o.get_frame('prostate_training')
test_frame = h2o.get_frame('prostate_validation')

# Model with maximum of 2 trees
model_2_trees = H2OXGBoostEstimator(training_frame=training_frame, learn_rate=0.7,
                                    booster='gbtree', seed=1, ntrees=2, distribution='bernoulli')
model_2_trees.train(x=x, y=y, training_frame=training_frame)
prediction_2_trees = model_2_trees.predict(test_frame)

assert prediction_2_trees.nrows == test_frame.nrows

# Model with 10 trees
model_10_trees = H2OXGBoostEstimator(training_frame=training_frame, learn_rate=0.7,
                                     booster='gbtree', seed=1, ntrees=10, distribution='bernoulli')
model_10_trees.train(x=x, y=y, training_frame=training_frame)
prediction_10_trees = model_10_trees.predict(test_frame)

assert prediction_10_trees.nrows == test_frame.nrows

## Performance of a model with 10 dec. trees should be better than model with 2 trees allowed only
assert model_2_trees.logloss(train=True) > model_10_trees.logloss(train=True)