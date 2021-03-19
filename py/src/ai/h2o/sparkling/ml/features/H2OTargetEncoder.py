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

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.H2OStageBase import H2OStageBase
from ai.h2o.sparkling.ml.Utils import Utils
from ai.h2o.sparkling.ml.models import H2OTargetEncoderModel
from ai.h2o.sparkling.ml.params.H2OTargetEncoderMOJOParams import H2OTargetEncoderMOJOParams
from pyspark import keyword_only
from pyspark.ml.wrapper import JavaEstimator


class H2OTargetEncoder(H2OTargetEncoderMOJOParams, H2OStageBase, JavaEstimator):

    @keyword_only
    def __init__(self,
                 foldCol=None,
                 labelCol="label",
                 inputCols=[],
                 outputCols=[],
                 holdoutStrategy="None",
                 blendedAvgEnabled=False,
                 blendedAvgInflectionPoint=10.0,
                 blendedAvgSmoothing=20.0,
                 noise=0.01,
                 noiseSeed=-1,
                 columnsToEncode=None,
                 problemType="Auto"):
        Initializer.load_sparkling_jar()
        super(H2OTargetEncoder, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.features.H2OTargetEncoder", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        if 'inputCols' in kwargs:
            kwargs['inputCols'] = self._convertInputCols(kwargs['inputCols'])
        self._set(**kwargs)
        self.setInputCols(self.getInputCols())

    def _create_model(self, java_model):
        return H2OTargetEncoderModel(java_model)

    def _toStringArray(self, value):
        if isinstance(value, list):
            return value
        else:
            return [str(value)]

    def _convertInputCols(self, value):
        if isinstance(value, list):
            return [self._toStringArray(item) for item in value]
        else:
            return value

    ##
    # Setters
    ##
    def setFoldCol(self, value):
        return self._set(foldCol=value)

    def setLabelCol(self, value):
        return self._set(labelCol=value)

    def setInputCols(self, value):
        return self._set(inputCols=self._convertInputCols(value))

    def setOutputCols(self, value):
        return self._set(outputCols=value)

    def setHoldoutStrategy(self, value):
        return self._set(holdoutStrategy=value)

    def setBlendedAvgEnabled(self, value):
        return self._set(blendedAvgEnabled=value)

    def setBlendedAvgInflectionPoint(self, value):
        return self._set(blendedAvgInflectionPoint=value)

    def setBlendedAvgSmoothing(self, value):
        return self._set(blendedAvgSmoothing=value)

    def setNoise(self, value):
        return self._set(noise=value)

    def setNoiseSeed(self, value):
        return self._set(noiseSeed=value)

    def setProblemType(self, value):
        return self._set(problemType=value)
