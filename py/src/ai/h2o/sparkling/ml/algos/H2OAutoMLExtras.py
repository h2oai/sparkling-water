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

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from ai.h2o.sparkling.ml.models.H2OMOJOModel import H2OMOJOModelFactory


class H2OAutoMLExtras:

    def getLeaderboard(self, *extraColumns):
        if len(extraColumns) == 1 and isinstance(extraColumns[0], list):
            extraColumns = extraColumns[0]
        leaderboard_java = self._java_obj.getLeaderboard(extraColumns)
        return DataFrame(leaderboard_java, SparkSession.builder.getOrCreate()._wrapped)

    def getAllModels(self):
        javaModels = self._java_obj.getAllModels()
        return [H2OMOJOModelFactory.createSpecificMOJOModel(javaModel) for javaModel in javaModels]
