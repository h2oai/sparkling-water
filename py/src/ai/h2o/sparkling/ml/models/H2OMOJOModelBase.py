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
from pyspark.ml.util import JavaMLWritable, JavaMLReadable
from pyspark.ml.wrapper import JavaModel


class H2OMOJOModelBase(JavaModel, JavaMLWritable, JavaMLReadable):

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
        return self._java_obj.getWithDetailedPredictionCol()

    def getFeaturesCols(self):
        return self._java_obj.getFeaturesCols()

    def getConvertUnknownCategoricalLevelsToNa(self):
        return self._java_obj.getConvertUnknownCategoricalLevelsToNa()

    def getConvertInvalidNumbersToNa(self):
        return self._java_obj.getConvertInvalidNumbersToNa()

    def getNamedMojoOutputColumns(self):
        return self._java_obj.getNamedMojoOutputColumns()
