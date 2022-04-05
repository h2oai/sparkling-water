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

from pyspark.ml.param import *
from ai.h2o.sparkling.ml.metrics.H2ORegressionMetricsBase import H2ORegressionMetricsBase
from ai.h2o.sparkling.Initializer import Initializer
from pyspark.ml.util import _jvm


class H2ORegressionMetrics(H2ORegressionMetricsBase):

    @staticmethod
    def calculate(dataFrame,
                  predictionCol = "detailed_prediction",
                  labelCol = "label",
                  weightCol = None,
                  offsetCol = None):
        '''
        The method calculates regression metrics on a provided data frame with predictions and actual values.
        :param dataFrame: A data frame with predictions and actual values
        :param predictionCol: The name of prediction column. The prediction column must have the same type as
        a detailed_prediction column coming from the transform method of H2OMOJOModel descendant or
        it must be of DoubleType or FloatType.
        :param labelCol: The name of label column that contains actual values.
        :param weightCol: The name of a weight column.
        :param offsetCol: The name of a offset column.
        :return: Calculated regression metrics
        '''
        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
        Initializer.load_sparkling_jar()
        javaMetrics = _jvm().ai.h2o.sparkling.ml.metrics.H2ORegressionMetrics.calculateInternal(dataFrame._jdf,
                                                                                                predictionCol,
                                                                                                labelCol,
                                                                                                weightCol,
                                                                                                offsetCol)
        return H2ORegressionMetrics(javaMetrics)
