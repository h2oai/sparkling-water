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
from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OAlgoSupervisedParams import H2OAlgoSupervisedParams
from ai.h2o.sparkling.ml.params.H2OTreeBasedSupervisedMOJOParams import H2OTreeBasedSupervisedMOJOParams
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from ai.h2o.sparkling.ml.params.HasStoppingCriteria import HasStoppingCriteria


class H2OSharedTreeParams(H2OAlgoSupervisedParams, H2OTreeBasedSupervisedMOJOParams, HasStoppingCriteria):
    ##
    # Param definitions
    ##
    maxDepth = Param(
        Params._dummy(),
        "maxDepth",
        "Maximum tree depth",
        H2OTypeConverters.toInt())

    minRows = Param(
        Params._dummy(),
        "minRows",
        "Fewest allowed (weighted) observations in a leaf",
        H2OTypeConverters.toFloat())

    nbins = Param(
        Params._dummy(),
        "nbins",
        "For numerical columns (real/int), build a histogram of (at least) this many bins, then split "
        "at the best point",
        H2OTypeConverters.toInt())

    nbinsCats = Param(
        Params._dummy(),
        "nbinsCats",
        "For categorical columns (factors), build a histogram of this many bins, then split at the best "
        "point. Higher values can lead to more overfitting",
        H2OTypeConverters.toInt())

    minSplitImprovement = Param(
        Params._dummy(),
        "minSplitImprovement",
        "Minimum relative improvement in squared error reduction for a split to happen",
        H2OTypeConverters.toFloat())

    histogramType = Param(
        Params._dummy(),
        "histogramType",
        "What type of histogram to use for finding optimal split points",
        H2OTypeConverters.toEnumString("hex.tree.SharedTreeModel$SharedTreeParameters$HistogramType"))

    r2Stopping = Param(
        Params._dummy(),
        "r2Stopping",
        "r2_stopping is no longer supported and will be ignored if set - please use stopping_rounds, "
        "stopping_metric and stopping_tolerance instead. Previous version of H2O would stop making trees "
        "when the R^2 metric equals or exceeds this",
        H2OTypeConverters.toFloat())

    nbinsTopLevel = Param(
        Params._dummy(),
        "nbinsTopLevel",
        "For numerical columns (real/int), build a histogram of (at most) this many bins at the root "
        "level, then decrease by factor of two per level",
        H2OTypeConverters.toInt())

    buildTreeOneNode = Param(
        Params._dummy(),
        "buildTreeOneNode",
        "Run on one node only; no network overhead but fewer cpus used.  Suitable for small datasets.",
        H2OTypeConverters.toBoolean())

    scoreTreeInterval = Param(
        Params._dummy(),
        "scoreTreeInterval",
        "Score the model after every so many trees. Disabled if set to 0.",
        H2OTypeConverters.toInt())
    sampleRate = Param(
        Params._dummy(),
        "sampleRate",
        "Row sample rate per tree (from 0.0 to 1.0)",
        H2OTypeConverters.toFloat())

    sampleRatePerClass = Param(
        Params._dummy(),
        "sampleRatePerClass",
        "A list of row sample rates per class (relative fraction for each class, from 0.0 to 1.0), for each tree",
        H2OTypeConverters.toNullableListFloat())

    colSampleRateChangePerLevel = Param(
        Params._dummy(),
        "colSampleRateChangePerLevel",
        "Relative change of the column sampling rate for every level (from 0.0 to 2.0)",
        H2OTypeConverters.toFloat())

    colSampleRatePerTree = Param(
        Params._dummy(),
        "colSampleRatePerTree",
        "Column sample rate per tree (from 0.0 to 1.0)",
        H2OTypeConverters.toFloat())

    ##
    # Getters
    ##
    def getMaxDepth(self):
        return self.getOrDefault(self.maxDepth)

    def getMinRows(self):
        return self.getOrDefault(self.minRows)

    def getNbins(self):
        return self.getOrDefault(self.nbins)

    def getNbinsCats(self):
        return self.getOrDefault(self.nbinsCats)

    def getMinSplitImprovement(self):
        return self.getOrDefault(self.minSplitImprovement)

    def getHistogramType(self):
        return self.getOrDefault(self.histogramType)

    def getR2Stopping(self):
        warnings.warn("The method 'getR2Stopping' is deprecated. " +
                      "Use 'getStoppingRounds', 'getStoppingMetric', 'getStoppingTolerance' instead!")
        return self.getOrDefault(self.r2Stopping)

    def getNbinsTopLevel(self):
        return self.getOrDefault(self.nbinsTopLevel)

    def getBuildTreeOneNode(self):
        return self.getOrDefault(self.buildTreeOneNode)

    def getScoreTreeInterval(self):
        return self.getOrDefault(self.scoreTreeInterval)

    def getSampleRate(self):
        return self.getOrDefault(self.sampleRate)

    def getSampleRatePerClass(self):
        return self.getOrDefault(self.sampleRatePerClass)

    def getColSampleRateChangePerLevel(self):
        return self.getOrDefault(self.colSampleRateChangePerLevel)

    def getColSampleRatePerTree(self):
        return self.getOrDefault(self.colSampleRatePerTree)

    ##
    # Setters
    ##
    def setNtrees(self, value):
        return self._set(ntrees=value)

    def setMaxDepth(self, value):
        return self._set(maxDepth=value)

    def setMinRows(self, value):
        return self._set(minRows=value)

    def setNbins(self, value):
        return self._set(nbins=value)

    def setNbinsCats(self, value):
        return self._set(nbinsCats=value)

    def setMinSplitImprovement(self, value):
        return self._set(minSplitImprovement=value)

    def setHistogramType(self, value):
        return self._set(histogramType=value)

    def setR2Stopping(self, value):
        warnings.warn("The method 'setR2Stopping' is deprecated. " +
                      "Use 'setStoppingRounds', 'setStoppingMetric', 'setStoppingTolerance' instead!")
        return self._set(r2Stopping=value)

    def setNbinsTopLevel(self, value):
        return self._set(nbinsTopLevel=value)

    def setBuildTreeOneNode(self, value):
        return self._set(buildTreeOneNode=value)

    def setScoreTreeInterval(self, value):
        return self._set(scoreTreeInterval=value)

    def setSampleRate(self, value):
        return self._set(sampleRate=value)

    def setSampleRatePerClass(self, value):
        return self._set(sampleRatePerClass=value)

    def setColSampleRateChangePerLevel(self, value):
        return self._set(colSampleRateChangePerLevel=value)

    def setColSampleRatePerTree(self, value):
        return self._set(colSampleRatePerTree=value)
