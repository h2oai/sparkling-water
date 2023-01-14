#!/usr/bin/env python3
import os
import glob
import subprocess
import sys
import logging as log
subprocess.run([sys.executable, "-m", "pip", "install", "h2o", "retry"])
from retry import retry
import h2o
from h2o.exceptions import H2OConnectionError
from h2o.estimators import H2OGradientBoostingEstimator

sparkling_water_zip = glob.glob('/sparkling_conductor_package/h2o-sparkling-water-*.tar.gz')[0]
tar_gz_file_name = os.path.basename(sparkling_water_zip)
h2o_version, _, sw_spark_version = tar_gz_file_name.lstrip('h2o-sparkling-water-').rstrip(".tar.gz").split("-")

@retry(H2OConnectionError, tries=10, delay=30)
def connect_to_h2o():
    h2o.connect(ip=os.environ['MANAGEMENT_HOST'], port=54321)
    h2o.connect(ip=os.environ['MANAGEMENT_HOST'], port=54322)
    h2o.connect(ip=os.environ['MANAGEMENT_HOST'], port=54323)
    h2o.connect(ip=os.environ['MANAGEMENT_HOST'], port=54324)
    h2o.connect(ip=os.environ['MANAGEMENT_HOST'], port=54325)

connect_to_h2o()
prostate = h2o.import_file("http://s3.amazonaws.com/h2o-public-test-data/smalldata/prostate/prostate.csv")
prostate["CAPSULE"] = prostate["CAPSULE"].asfactor()
predictors = ["ID", "AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]
response = "CAPSULE"
pros_gbm = H2OGradientBoostingEstimator(nfolds=5, seed=1111)
pros_gbm.train(x=predictors, y=response, training_frame=prostate)
pred = pros_gbm.predict(prostate)
log.info("smoke test success")
