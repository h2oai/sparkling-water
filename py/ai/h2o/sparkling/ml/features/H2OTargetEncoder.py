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

from pyspark.ml.util import JavaMLWritable, JavaMLReadable
from pyspark.ml.wrapper import JavaEstimator
from pyspark.sql import SparkSession
from pyspark import keyword_only
from py_sparkling.ml.util import getValidatedEnumValue, validateEnumValue

from pysparkling.context import H2OContext
from pysparkling.spark_specifics import get_input_kwargs
from ai.h2o.sparkling.ml.params import H2OTargetEncoderParams
from py_sparkling.ml.util import set_double_values
from ai.h2o.sparkling.ml.models import H2OTargetEncoderModel

from h2o.utils.typechecks import assert_is_type, Enum


class H2OTargetEncoder(H2OTargetEncoderParams, JavaEstimator, JavaMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, foldCol=None, labelCol="label", inputCols=[], holdoutStrategy = "None",
                 blendedAvgEnabled=False, blendedAvgInflectionPoint=10.0, blendedAvgSmoothing=20.0, noise=0.01, noiseSeed=-1):
        super(H2OTargetEncoder, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.features.H2OTargetEncoder", self.uid)

        self._setDefault(foldCol=None, labelCol="label", inputCols=[], holdoutStrategy="None",
                         blendedAvgEnabled=False, blendedAvgInflectionPoint=10.0, blendedAvgSmoothing=20.0, noise=0.01, noiseSeed=-1)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)


    @keyword_only
    def setParams(self, foldCol=None, labelCol="label", inputCols=[], holdoutStrategy = "None",
                  blendedAvgEnabled=False, blendedAvgInflectionPoint=10.0, blendedAvgSmoothing=20.0, noise=0.01, noiseSeed=-1):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self.__getHoldoutStrategyEnumName(), kwargs, "holdoutStrategy")

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["blendedAvgInflectionPoint", "blendedAvgSmoothing", "noise"]
        set_double_values(kwargs, double_types)

        return self._set(**kwargs)

    def __getHoldoutStrategyEnumName(self):
        return "ai.h2o.sparkling.ml.features.H2OTargetEncoderHoldoutStrategy"

    def _create_model(self, java_model):
        return H2OTargetEncoderModel(java_model)


    ##
    # Setters
    ##

    def setFoldCol(self, value):
        assert_is_type(value, None, str)
        return self._set(foldCol=value)

    def setLabelCol(self, value):
        assert_is_type(value, str)
        return self._set(labelCol=value)

    def setInputCols(self, value):
        assert_is_type(value, [str])
        return self._set(inputCols=value)

    def setHoldoutStrategy(self, value):
        validated = getValidatedEnumValue(self.__getHoldoutStrategyEnumName(), value)
        return self._set(holdoutStrategy=validated)

    def setBlendedAvgEnabled(self, value):
        assert_is_type(value, bool)
        return self._set(blendedAvgEnabled=value)

    def setBlendedAvgInflectionPoint(self, value):
        assert_is_type(value, int, float)
        return self._set(blendedAvgInflectionPoint=float(value))

    def setBlendedAvgSmoothing(self, value):
        assert_is_type(value, int, float)
        return self._set(blendedAvgSmoothing=float(value))

    def setNoise(self, value):
        assert_is_type(value, int, float)
        return self._set(noise=float(value))

    def setNoiseSeed(self, value):
        assert_is_type(value, int)
        return self._set(noiseSeed=value)
