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

import warnings

from pyspark.sql import SparkSession
from pyspark import keyword_only
from pyspark.sql.dataframe import DataFrame

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.Utils import Utils
from ai.h2o.sparkling.ml.algos.H2OSupervisedAlgoBase import H2OSupervisedAlgoBase
from ai.h2o.sparkling.ml.params import H2OAutoMLParams


class H2OAutoML(H2OAutoMLParams, H2OSupervisedAlgoBase):

    @keyword_only
    def __init__(self,
                 ignoredCols=[],
                 includeAlgos=["GLM", "DRF", "GBM", "DeepLearning", "StackedEnsemble", "XGBoost"],
                 excludeAlgos=[],
                 projectName=None,
                 maxRuntimeSecs=0.0,
                 stoppingRounds=3,
                 stoppingTolerance=0.001,
                 stoppingMetric="AUTO",
                 sortMetric="AUTO",
                 balanceClasses=False,
                 classSamplingFactors=None,
                 maxAfterBalanceSize=5.0,
                 keepCrossValidationPredictions=False,
                 keepCrossValidationModels=False,
                 maxModels=0,
                 labelCol="label",
                 foldCol=None,
                 weightCol=None,
                 offsetCol=None,
                 splitRatio=1.0,
                 seed=-1,
                 nfolds=5,
                 allStringColumnsToCategorical=True,
                 columnsToCategorical=[],
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False,
                 featuresCols=[],
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False,
                 namedMojoOutputColumns=True,
                 monotoneConstraints={}):
        Initializer.load_sparkling_jar()
        super(H2OAutoML, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OAutoML", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        self._set(**kwargs)

    def leaderboard(self):
        warnings.warn("The 'leaderboard' method is deprecated and will be removed in the SW version 3.30. "
                      "Use the 'getLeaderboard' method instead!")
        leaderboard_java = self._java_obj.leaderboard()
        if leaderboard_java.isDefined():
            return DataFrame(leaderboard_java.get(), SparkSession.builder.getOrCreate()._wrapped)
        else:
            return None

    def getLeaderboard(self, *extraColumns):
        if len(extraColumns) == 1 and isinstance(extraColumns[0], list):
            extraColumns = extraColumns[0]
        leaderboard_java = self._java_obj.getLeaderboard(extraColumns)
        return DataFrame(leaderboard_java, SparkSession.builder.getOrCreate()._wrapped)
