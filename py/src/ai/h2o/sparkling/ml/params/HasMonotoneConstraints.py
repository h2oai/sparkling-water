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

from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters


class HasMonotoneConstraints(Params):
    monotoneConstraints = Param(
        Params._dummy(),
        "monotoneConstraints",
        "Monotone Constraints - A key must correspond to a feature name and value could be 1 or -1",
        H2OTypeConverters.toDictionaryWithFloatElements())

    def getMonotoneConstraints(self):
        return self.getOrDefault(self.monotoneConstraints)

    def setMonotoneConstraints(self, value):
        return self._set(monotoneConstraints=value)
