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
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.wrapper import JavaTransformer


class H2OWord2VecTokenizer(H2OStageBase, JavaTransformer):
    stopWords = Param(
        Params._dummy(),
        "stopWords",
        "Remove specified words from the array vector in the dataset",
        H2OTypeConverters.toNullableListString())

    inputCol = Param(
        Params._dummy(),
        "inputCol",
        "Input Column",
        H2OTypeConverters.toNullableString())

    outputCol = Param(
        Params._dummy(),
        "outputCol",
        "Output Column",
        H2OTypeConverters.toNullableString())

    @keyword_only
    def __init__(self,
                 stopWords=False,
                 inputCol=None,
                 outputCol=None):
        Initializer.load_sparkling_jar()
        super(H2OWord2VecTokenizer, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.features.H2OWord2VecTokenizer", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        self._set(**kwargs)

    def setStopWords(self, value):
        return self._set(stopWords=value)

    def setInputCol(self, value):
        return self._set(inputCol=value)

    def setOutputCol(self, value):
        return self._set(outputCol=value)

    def getStopWordds(self):
        return self.getOrDefault(self.stopWords)

    def getInputCol(self):
        return self.getOrDefault(self.inputCol)

    def getOutputCol(self):
        return self.getOrDefault(self.outputCol)
