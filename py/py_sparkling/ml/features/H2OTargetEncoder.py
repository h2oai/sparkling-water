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

from pyspark.ml.util import JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator
from pyspark.sql import SparkSession

from pysparkling.context import H2OContext
from pysparkling.spark_specifics import get_input_kwargs
from pysparkling.ml.params import H2OTargetEncoderParams
from py_sparkling.ml.util import set_double_values, JavaH2OMLReadable
from py_sparkling.ml.util import get_correct_case_enum
from py_sparkling.ml.models import H2OTargetEncoderModel

from h2o.utils.typechecks import assert_is_type, Enum


class H2OTargetEncoder(H2OTargetEncoderParams, JavaEstimator, JavaH2OMLReadable, JavaMLWritable):

    @keyword_only
    def __init__(self, foldCol=None, labelCol="label", inputCols=[], houldoutStragegy = "None",
                 blendedAvgEnabled=False, blendedAvgInflectionPoint=10.0, blendedAvgSmoothing=20.0, noise=0.01, noiseSeed=-1):
        super(H2OTargetEncoder, self).__init__()
        self._hc = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)
        self._java_obj = self._new_java_obj("py_sparkling.ml.features.H2OTargetEncoder", self.uid)

        self._setDefault(foldCol=None, labelCold="label", inputCols=[], houldoutStragegy=self._hc._jvm.org.apache.spark.ml.h2o.features.H2OTargetEncoderHoldoutStrategy.valueOf("None"),
                         blendedAvgEnabled=False, blendedAvgInflectionPoint=10.0, blendedAvgSmoothing=20.0, noise=0.01, noiseSeed=-1)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)


    @keyword_only
    def setParams(self, foldCol=None, labelCold="label", inputCols=[], houldoutStragegy = "None",
                  blendedAvgEnabled=False, blendedAvgInflectionPoint=10.0, blendedAvgSmoothing=20.0, noise=0.01, noiseSeed=-1):
        kwargs = get_input_kwargs(self)

        if "houldoutStragegy" in kwargs:
            kwargs["houldoutStragegy"] = self._hc._jvm.org.apache.spark.ml.h2o.features.H2OTargetEncoderHoldoutStrategy.valueOf(kwargs["houldoutStragegy"])

        # we need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["blendedAvgInflectionPoint", "blendedAvgSmoothing", "noise"]
        set_double_values(kwargs, double_types)

        return self._set(**kwargs)


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
        assert_is_type(value, None, Enum("LeaveOneOut", "KFold", "None"))
        if value is None:
            value = "None"
        jvm = H2OContext.getOrCreate(SparkSession.builder.getOrCreate(), verbose=False)._jvm
        correct_enum_value = get_correct_case_enum(jvm.org.apache.spark.ml.h2o.features.H2OTargetEncoderHoldoutStrategy.values(), value)
        return self._set(holdoutStrategy=correct_enum_value)

    def setBlendedAvgEnabled(self, value):
        assert_is_type(value, bool)
        return self._set(blendedAvgEnabled=value)

    def setBlendedAvgInflectionPoint(self, value):
        assert_is_type(value, numeric)
        return self._set(blendedAvgInflectionPoint=value)

    def setBlendedAvgSmoothing(self, value):
        assert_is_type(value, numeric)
        return self._set(blendedAvgSmoothing=value)

    def setNoise(self, value):
        assert_is_type(value, numeric)
        return self._set(noise=value)

    def setNoiseSeed(self, value):
        assert_is_type(value, int)
        return self._set(noiseSeed=value)
