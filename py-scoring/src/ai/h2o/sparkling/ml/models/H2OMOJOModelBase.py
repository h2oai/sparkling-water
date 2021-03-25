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
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.wrapper import JavaModel
from ai.h2o.sparkling.ml.util.H2OJavaMLReadable import H2OJavaMLReadable
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters

import warnings


class H2OMOJOModelBase(JavaModel, JavaMLWritable, H2OJavaMLReadable):

    # Overriding the method to avoid changes on the companion Java object
    def _transfer_params_to_java(self):
        pass

    ##
    # Getters
    ##
    def getPredictionCol(self):
        return self._java_obj.getPredictionCol()

    def getDetailedPredictionCol(self):
        return self._java_obj.getDetailedPredictionCol()

    def getWithDetailedPredictionCol(self):
        warnings.warn("The method will be removed without a replacement in the version 3.36."
                      "Detailed prediction columns is always enabled.", DeprecationWarning)
        return True

    def getFeaturesCols(self):
        return list(self._java_obj.getFeaturesCols())

    def getFeatureTypes(self):
        return H2OTypeConverters.scalaMapStringStringToDictStringAny(self._java_obj.getFeatureTypes())

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self._java_obj.getConvertUnknownCategoricalLevelsToNa()

    def getConvertInvalidNumbersToNa(self):
        return self._java_obj.getConvertInvalidNumbersToNa()

    def getNamedMojoOutputColumns(self):
        return self._java_obj.getNamedMojoOutputColumns()

    def getWithContributions(self):
        return self._java_obj.getWithContributions()

    def getWithLeafNodeAssignments(self):
        return self._java_obj.getWithLeafNodeAssignments()

    def getWithStageResults(self):
        return self._java_obj.getWithStageResults()
