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
from ai.h2o.sparkling.ml.params.HasInputColsOnMOJO import HasInputColsOnMOJO
from ai.h2o.sparkling.ml.params.HasOutputColOnMOJO import HasOutputColOnMOJO


class H2OAutoEncoderExtraParamsOnMOJO(HasInputColsOnMOJO, HasOutputColOnMOJO):
    def getOriginalCol(self):
        return self._java_obj.getOriginalCol()

    def getWithOriginalCol(self):
        return self._java_obj.getWithOriginalCol()

    def getMSECol(self):
        return self._java_obj.getMSECol()

    def getWithMSECol(self):
        return self._java_obj.getWithMSECol()

    def setOriginalCol(self, value):
        self._java_obj.setOriginalCol(value)
        return self

    def setWithOriginalCol(self, value):
        self._java_obj.setWithOriginalCol(value)
        return self

    def setMSECol(self, value):
        self._java_obj.setMSECol(value)
        return self

    def setWithMSECol(self, value):
        self._java_obj.setWithMSECol(value)
        return self
