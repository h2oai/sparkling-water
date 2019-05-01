#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for PySparkling Data Conversions;
"""
import sys
import os

sys.path.insert(0, sys.argv[1])
os.environ['PYSPARK_PYTHON'] = sys.executable
import unittest
from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf

from pyspark.sql import SparkSession

import h2o
import unit_test_utils
import generic_test_utils
import time
import os
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml.algos import H2OGLM, H2OGridSearch, H2OGBM
from pyspark.ml import Pipeline, PipelineModel


# Test of transformations from dataframe/rdd to h2o frame and from h2o frame back to dataframe
class FrameTransformationsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._conf = unit_test_utils.get_default_spark_conf(cls._spark_options_from_params)
        cls._spark = SparkSession.builder.config(conf=cls._conf).getOrCreate()
        cls._hc = H2OContext.getOrCreate(cls._spark, H2OConf(cls._spark).set_num_of_external_h2o_nodes(1))

    # test transformation from dataframe to h2o frame
    def test_df_to_h2o_frame(self):
        hc = self._hc
        df = self._spark.sparkContext.parallelize([(num, "text") for num in range(0, 100)]).toDF()
        h2o_frame = hc.as_h2o_frame(df)
        self.assertEquals(h2o_frame.nrow, df.count(), "Number of rows should match")
        self.assertEquals(h2o_frame.ncol, len(df.columns), "Number of columns should match")
        self.assertEquals(h2o_frame.names, df.columns, "Column names should match")
        self.assertEquals(df.first()._2, "text", "Value should match")

    # test transformation from wide dataframe to h2o frame and edit it
    def test_wide_df_to_h2o_frame_and_edit(self):
        n_col = 110
        test_data_frame = self._spark.createDataFrame([tuple(range(n_col))])
        h2o_frame = self._hc.as_h2o_frame(test_data_frame)
        self.assertEquals(h2o_frame.dim[1], n_col, "Number of cols should match")
        self.assertEquals(h2o_frame['_107'], 107, "Content of columns should be the same")
        # h2o_frame.refresh()     # this helps to pass the test
        # in commit f50dd728281d11f9a2ab3cdaeb994644b892d65a
        col_102 = '_102'
        # replace a column after the column 100
        h2o_frame[col_102] = h2o_frame[col_102].asfactor()
        h2o_frame.refresh()
        self.assertEquals(h2o_frame.dim[1], n_col, "Number of cols after replace should match")

    # test transformation from RDD consisting of python integers to h2o frame
    def test_rdd_int_h2o_frame(self):
        hc = self._hc
        rdd = self._spark.sparkContext.parallelize([num for num in range(0, 100)])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0, 0], 0, "Value should match")
        unit_test_utils.asert_h2o_frame(self, h2o_frame, rdd)

    # test transformation from RDD consisting of python booleans to h2o frame
    def test_rdd_bool_to_h2o_frame(self):
        hc = self._hc
        rdd = self._spark.sparkContext.parallelize([True, False, True, True, False])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0, 0], 1, "Value should match")
        self.assertEquals(h2o_frame[1, 0], 0, "Value should match")
        unit_test_utils.asert_h2o_frame(self, h2o_frame, rdd)

    # test transformation from RDD consisting of python strings to h2o frame
    def test_rdd_str_h2o_frame(self):
        hc = self._hc
        rdd = self._spark.sparkContext.parallelize(["a", "b", "c"])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0, 0], "a", "Value should match")
        self.assertEquals(h2o_frame[2, 0], "c", "Value should match")
        unit_test_utils.asert_h2o_frame(self, h2o_frame, rdd)

    # test transformation from RDD consisting of python floats to h2o frame
    def test_rdd_float_h2o_frame(self):
        hc = self._hc
        rdd = self._spark.sparkContext.parallelize([0.5, 1.3333333333, 178])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0, 0], 0.5, "Value should match")
        self.assertEquals(h2o_frame[1, 0], 1.3333333333, "Value should match")
        unit_test_utils.asert_h2o_frame(self, h2o_frame, rdd)

    # test transformation from RDD consisting of python doubles to h2o frame
    def test_rdd_double_h2o_frame(self):
        hc = self._hc
        rdd = self._spark.sparkContext.parallelize([0.5, 1.3333333333, 178])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0, 0], 0.5, "Value should match")
        self.assertEquals(h2o_frame[1, 0], 1.3333333333, "Value should match")
        unit_test_utils.asert_h2o_frame(self, h2o_frame, rdd)

    # test transformation from RDD consisting of python complex types to h2o frame
    def test_rdd_complex_h2o_frame_1(self):
        hc = self._hc
        rdd = self._spark.sparkContext.parallelize([("a", 1, 0.5), ("b", 2, 1.5)])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0, 0], "a", "Value should match")
        self.assertEquals(h2o_frame[1, 0], "b", "Value should match")
        self.assertEquals(h2o_frame[1, 2], 1.5, "Value should match")
        self.assertEquals(h2o_frame.nrow, rdd.count(), "Number of rows should match")
        self.assertEquals(h2o_frame.ncol, 3, "Number of columns should match")
        self.assertEquals(h2o_frame.names, ["_1", "_2", "_3"], "Column names should match")

    # test transformation from RDD consisting of python long to h2o frame
    def test_rdd_long_h2o_frame(self):
        hc = self._hc
        min = hc._jvm.Integer.MIN_VALUE - 1
        max = hc._jvm.Integer.MAX_VALUE + 1
        rdd = self._spark.sparkContext.parallelize([1, min, max])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0, 0], 1, "Value should match")
        self.assertEquals(h2o_frame[1, 0], min, "Value should match")
        self.assertEquals(h2o_frame[2, 0], max, "Value should match")
        unit_test_utils.asert_h2o_frame(self, h2o_frame, rdd)

    # test transformation of numeric too big to be handled by standard java types
    def test_rdd_numeric_too_big_h2o_frame(self):
        hc = self._hc
        min = hc._jvm.Long.MIN_VALUE - 1
        max = hc._jvm.Long.MAX_VALUE + 1
        rdd = self._spark.sparkContext.parallelize([1, min, max])
        with self.assertRaises(ValueError):
            h2o_frame = hc.as_h2o_frame(rdd)

    # test transformation from h2o frame to data frame, when given h2o frame was created without calling as_h2o_frame
    # on h2o context
    def test_h2o_frame_2_data_frame_new(self):
        hc = self._hc
        h2o_frame = h2o.upload_file(generic_test_utils.locate("smalldata/prostate/prostate.csv"))
        df = hc.as_spark_frame(h2o_frame)
        self.assertEquals(df.count(), h2o_frame.nrow, "Number of rows should match")
        self.assertEquals(len(df.columns), h2o_frame.ncol, "Number of columns should match")
        self.assertEquals(df.columns, h2o_frame.names, "Column names should match")

    def test_h2o_frame_2_data_frame_second_conversion(self):
        hc = self._hc
        h2o_frame = h2o.upload_file(generic_test_utils.locate("smalldata/prostate/prostate.csv"))
        df1 = hc.as_spark_frame(h2o_frame)
        df2 = hc.as_spark_frame(h2o_frame)
        self.assertEquals(df1.count(), df2.count(), "Number of rows should match")
        self.assertEquals(len(df1.columns), len(df2.columns), "Number of columns should match")
        self.assertEquals(df1.columns, df2.columns, "Column names should match")

    # test transformation from h2o frame to data frame, when given h2o frame was obtained using as_h2o_frame method
    # on h2o context
    def test_h2o_frame_2_data_frame_2(self):
        hc = self._hc
        rdd = self._spark.sparkContext.parallelize(["a", "b", "c"])
        h2o_frame = hc.as_h2o_frame(rdd)
        df = hc.as_spark_frame(h2o_frame)
        self.assertEquals(df.count(), h2o_frame.nrow, "Number of rows should match")
        self.assertEquals(len(df.columns), h2o_frame.ncol, "Number of columns should match")
        self.assertEquals(df.columns, h2o_frame.names, "Column names should match")

    # test for SW-321
    def test_inner_cbind_transform(self):
        hc = self._hc
        import h2o
        h2o_df1 = h2o.H2OFrame({'A': [1, 2, 3]})
        h2o_df2 = h2o.H2OFrame({'B': [4, 5, 6]})
        spark_frame = hc.as_spark_frame(h2o_df1.cbind(h2o_df2))
        count = spark_frame.count()
        self.assertEquals(count, 3, "Number of rows is 3")

    # test for SW-430
    def test_lazy_frames(self):
        from pyspark.sql import Row
        hc = self._hc
        data = [Row(c1=1, c2="first"), Row(c1=2, c2="second")]
        df = self._spark.createDataFrame(data)
        hf = hc.as_h2o_frame(df)
        # Modify H2O frame - this should invalidate internal cache
        hf['c3'] = 3
        # Now try to convert modified H2O frame back to Spark data frame
        dfe = hc.as_spark_frame(hf)
        self.assertEquals(dfe.count(), len(data), "Number of rows should match")
        self.assertEquals(len(dfe.columns), 3, "Number of columns should match")
        self.assertEquals(dfe.collect(), [Row(c1=1, c2='first', c3=3), Row(c1=2, c2='second', c3=3)])

    def test_sparse_data_conversion(self):
        data = [(float(x), SparseVector(5000, {x: float(x)})) for x in range(1, 90)]
        df = self._spark.sparkContext.parallelize(data).toDF()
        t0 = time.time()
        self._hc.as_h2o_frame(df)
        total = time.time() - t0
        self.assertTrue(total < 20)  # The conversion should not take longer then 20 seconds

    def test_load_mojo_gbm(self):
        from pysparkling.ml import H2OMOJOModel, H2OGBM
        mojo = H2OMOJOModel.create_from_mojo(
            "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
        prostate_frame = self._hc.as_spark_frame(
            h2o.upload_file(unit_test_utils.locate("smalldata/prostate/prostate.csv")))

        gbm = H2OGBM(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule")

        model = gbm.fit(prostate_frame)

        pred_mojo = mojo.predict(prostate_frame).repartition(1).collect()
        pred_model = model.transform(prostate_frame).repartition(1).collect()

        self.assertEquals(len(pred_mojo), len(pred_model))
        num_cols = len(pred_mojo[0])
        for i in range(0, len(pred_mojo)):
            for k in range(0, num_cols - 2):
                self.assertEquals(pred_mojo[i][k], pred_model[i][k])
            self.assertEquals(pred_mojo[i][num_cols - 1][0], pred_model[i][num_cols - 1][0])
            self.assertEquals(pred_mojo[i][num_cols - 1][1], pred_model[i][num_cols - 1][1])

    def test_load_mojo_deeplearning(self):
        from pysparkling.ml import H2OMOJOModel, H2ODeepLearning
        mojo = H2OMOJOModel.create_from_mojo(
            "file://" + os.path.abspath("../ml/src/test/resources/deep_learning_prostate.mojo"))
        prostate_frame = self._hc.as_spark_frame(
            h2o.upload_file(unit_test_utils.locate("smalldata/prostate/prostate.csv")))

        dl = H2ODeepLearning(seed=42, reproducible=True, labelCol="CAPSULE")

        model = dl.fit(prostate_frame)

        pred_mojo = mojo.predict(prostate_frame).repartition(1).collect()
        pred_model = model.transform(prostate_frame).repartition(1).collect()

        self.assertEquals(len(pred_mojo), len(pred_model))
        for i in range(0, len(pred_mojo)):
            self.assertEquals(pred_mojo[i], pred_model[i])

    def test_simple_parquet_import(self):
        df = self._spark.sparkContext.parallelize([(num, "text") for num in range(0, 100)]).toDF().coalesce(1)
        df.write.mode('overwrite').parquet("file://" + os.path.abspath("build/tests_tmp/test.parquet"))

        parquet_file = None
        for file in os.listdir(os.path.abspath("build/tests_tmp/test.parquet")):
            if file.endswith(".parquet"):
                # it is always set
                parquet_file = file
        frame = h2o.upload_file(path=os.path.abspath("build/tests_tmp/test.parquet/" + parquet_file))
        self.assertEquals(frame.ncols, len(df.columns))
        self.assertEquals(frame.nrows, df.count())
        self.assertEquals(frame[0, 0], 0.0)
        self.assertEquals(frame[0, 1], "text")

    def test_unknown_type_conversion(self):
        with self.assertRaises(ValueError):
            self._hc.as_h2o_frame("unknown type")

    def test_convert_empty_dataframe_empty_schema(self):
        schema = StructType([])
        empty = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), schema)
        fr = self._hc.as_h2o_frame(empty)
        self.assertEquals(fr.nrows, 0)
        self.assertEquals(fr.ncols, 0)

    def test_convert_empty_dataframe_non_empty_schema(self):
        schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
        empty = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), schema)
        fr = self._hc.as_h2o_frame(empty)
        self.assertEquals(fr.nrows, 0)
        self.assertEquals(fr.ncols, 2)

    def test_convert_empty_rdd(self):
        schema = StructType([])
        empty = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), schema)
        fr = self._hc.as_h2o_frame(empty)
        self.assertEquals(fr.nrows, 0)
        self.assertEquals(fr.ncols, 0)

    def test_import_gcs(self):
        fr = h2o.import_file(
            "gs://gcp-public-data-nexrad-l2/2018/01/01/KABR/NWS_NEXRAD_NXL2DPBL_KABR_20180101050000_20180101055959.tar")

    def test_glm_in_spark_pipeline(self):
        prostate_frame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True, inferSchema=True)

        algo = H2OGLM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                      labelCol="AGE",
                      seed=1,
                      ratio=0.8)

        pipeline = Pipeline(stages=[algo])
        pipeline.write().overwrite().save("file://" + os.path.abspath("build/glm_pipeline"))
        loaded_pipeline = Pipeline.load("file://" + os.path.abspath("build/glm_pipeline"))
        model = loaded_pipeline.fit(prostate_frame)

        model.write().overwrite().save("file://" + os.path.abspath("build/glm_pipeline_model"))
        loaded_model = PipelineModel.load("file://" + os.path.abspath("build/glm_pipeline_model"))

        loaded_model.transform(prostate_frame).count()

    def test_grid_gbm_in_spark_pipeline(self):
        prostate_frame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True, inferSchema=True)

        algo = H2OGridSearch(labelCol="AGE", hyperParameters={"_seed": [1, 2, 3]}, ratio=0.8, algo=H2OGBM(),
                             strategy="RandomDiscrete", maxModels=3, maxRuntimeSecs=60, selectBestModelBy="RMSE")

        pipeline = Pipeline(stages=[algo])
        pipeline.write().overwrite().save("file://" + os.path.abspath("build/grid_gbm_pipeline"))
        loaded_pipeline = Pipeline.load("file://" + os.path.abspath("build/grid_gbm_pipeline"))
        model = loaded_pipeline.fit(prostate_frame)

        model.write().overwrite().save("file://" + os.path.abspath("build/grid_gbm_pipeline_model"))
        loaded_model = PipelineModel.load("file://" + os.path.abspath("build/grid_gbm_pipeline_model"))

        loaded_model.transform(prostate_frame).count()

    def test_custom_metric(self):
        from custom_metric_class import WeightedFalseNegativeLossMetric
        train_path = "file://" + unit_test_utils.locate("smalldata/loan.csv")
        train = h2o.import_file(train_path, destination_frame="loan_train")
        train["bad_loan"] = train["bad_loan"].asfactor()

        y = "bad_loan"
        x = train.col_names
        x.remove(y)
        x.remove("int_rate")

        train["weight"] = train["loan_amnt"]

        weighted_false_negative_loss_func = h2o.upload_custom_metric(WeightedFalseNegativeLossMetric,
                                                                     func_name="WeightedFalseNegativeLoss",
                                                                     func_file="weighted_false_negative_loss.py")
        from h2o.estimators import H2OGradientBoostingEstimator
        gbm = H2OGradientBoostingEstimator(model_id="gbm.hex", custom_metric_func=weighted_false_negative_loss_func)
        gbm.train(y=y, x=x, training_frame=train, weights_column="weight")

        perf = gbm.model_performance()
        self.assertEquals(perf.custom_metric_name(), "WeightedFalseNegativeLoss")
        self.assertEquals(perf.custom_metric_value(), 0.24579011595430142)


if __name__ == '__main__':
    generic_test_utils.run_tests([FrameTransformationsTest], file_name="py_unit_tests_conversions_report")
