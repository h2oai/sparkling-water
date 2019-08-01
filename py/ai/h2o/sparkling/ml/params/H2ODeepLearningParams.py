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

from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *

from ai.h2o.sparkling.ml.params.H2OAlgoSupervisedParams import H2OAlgoSupervisedParams


class H2ODeepLearningParams(H2OAlgoSupervisedParams):

    ##
    # Param definitions
    ##
    epochs = Param(Params._dummy(), "epochs", "The number of passes over the training dataset to be carried out")

    l1 = Param(Params._dummy(), "l1", "A regularization method that constrains the absolute value of the weights and"
                                      " has the net effect of dropping some weights (setting them to zero) from"
                                      " a model to reduce complexity and avoid overfitting.")

    l2 = Param(Params._dummy(), "l2", "A regularization method that constrains the sum of the squared weights."
                                      " This method introduces bias into parameter estimates, but frequently"
                                      " produces substantial gains in modeling as estimate variance is reduced.")

    hidden = Param(Params._dummy(), "hidden", "The number and size of each hidden layer in the model")
    reproducible = Param(Params._dummy(), "reproducible", "Force reproducibility on small data (will be slow - only uses 1 thread)")

    ##
    # Getters
    ##
    def getEpochs(self):
        return self.getOrDefault(self.epochs)

    def getL1(self):
        return self.getOrDefault(self.l1)

    def getL2(self):
        return self.getOrDefault(self.l2)

    def getHidden(self):
        return self.getOrDefault(self.hidden)

    def getReproducible(self):
        return self.getOrDefault(self.reproducible)

    ##
    # Setters
    ##
    def setEpochs(self, value):
        assert_is_type(value, int, float)
        return self._set(epochs=float(value))

    def setL1(self, value):
        assert_is_type(value, int, float)
        return self._set(l1=float(value))

    def setL2(self, value):
        assert_is_type(value, int, float)
        return self._set(l2=float(value))

    def setHidden(self, value):
        assert_is_type(value, [int])
        return self._set(hidden=value)

    def setReproducible(self, value):
        assert_is_type(value, bool)
        return self._set(reproducible=value)

