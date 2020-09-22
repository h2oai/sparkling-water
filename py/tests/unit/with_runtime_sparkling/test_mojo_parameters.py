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

from pysparkling.ml import H2OGBM, H2ODRF, H2OXGBoost, H2OGLM, H2OGAM
from pysparkling.ml import H2ODeepLearning, H2OKMeans, H2OGLRM, H2OPCA, H2OIsolationForest

def testGBMParameters(prostateDataset):
    algorithm = H2OGBM(seed=1, labelCol="CAPSULE")
    model = algorithm.fit(prostateDataset)
    compareParameterValues(algorithm, model)


def compareParameterValues(algorithm, model):
    methods = filter(lambda x: x.startswith("get"), dir(model))
    for method in methods:
        print(method)
        modelValue = getattr(model, method)()
        algorithmValue = getattr(algorithm, method)()
        assert(valuesAraEqual(modelValue, algorithmValue))

def valuesAraEqual(algorithmValue, modelValue):
    if algorithmValue == "AUTO":
        return True
    elif algorithmValue == "auto":
        return True
    elif algorithmValue == "family_default":
        return True
    else:
        return algorithmValue == modelValue


