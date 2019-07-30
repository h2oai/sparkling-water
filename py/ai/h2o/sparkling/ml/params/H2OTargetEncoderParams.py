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


class H2OTargetEncoderParams(Params):

    ##
    # Param definitions
    ##
    foldCol = Param(Params._dummy(), "foldCol", "Fold column name")
    labelCol = Param(Params._dummy(), "labelCol", "Label column name")
    inputCols = Param(Params._dummy(), "inputCols", "Names of columns that will be transformed")
    holdoutStrategy = Param(Params._dummy(), "holdoutStrategy",
    """A strategy deciding what records will be excluded when calculating the target average on the training dataset.
       Options:
        None        - All rows are considered for the calculation
        LeaveOneOut - All rows except the row the calculation is made for
        KFold       - Only out-of-fold data is considered (The option requires foldCol to be set.""")
    blendedAvgEnabled = Param(Params._dummy(), "blendedAvgEnabled",
    """If set, the target average becomes a weighted average of the posterior average for a given categorical level and the prior average of the target.
       The weight is determined by the size of the given group that the row belongs to. By default, the blended average is disabled.""")
    blendedAvgInflectionPoint = Param(Params._dummy(), "blendedAvgInflectionPoint",
    """A parameter of the blended average. The bigger number is set, the groups relatively bigger to the overall data set size will consider 
       the global target value as a component in the weighted average. The default value is 10.""")
    blendedAvgSmoothing = Param(Params._dummy(), "blendedAvgSmoothing",
    "A parameter of blended average. Controls the rate of transition between a group target value and a global target value. The default value is 20.")
    noise = Param(Params._dummy(), "noise", "Amount of random noise added to output values. The default value is 0.01")
    noiseSeed = Param(Params._dummy(), "noiseSeed", "A seed of the generator producing the random noise")

    ##
    # Getters
    ##
    def getFoldCol(self):
        return self.getOrDefault(self.foldCol)

    def getLabelCol(self):
        return self.getOrDefault(self.labelCol)

    def getInputCols(self):
        return self.getOrDefault(self.inputCols)

    def getOutputCols(self):
        return list(map(lambda c: c + "_te", self.getInputCols()))

    def getHoldoutStrategy(self):
        return self.getOrDefault(self.holdoutStrategy)

    def getBlendedAvgEnabled(self):
        return self.getOrDefault(self.blendedAvgEnabled)

    def getBlendedAvgInflectionPoint(self):
        return self.getOrDefault(self.blendedAvgInflectionPoint)

    def getBlendedAvgSmoothing(self):
        return self.getOrDefault(self.blendedAvgSmoothing)

    def getNoise(self):
        return self.getOrDefault(self.noise)

    def getNoiseSeed(self):
        return self.getOrDefault(self.noiseSeed)
