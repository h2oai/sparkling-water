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

from pysparkling.ml import H2OGBM, H2ODRF, H2OXGBoost, H2OGLM, H2OGAM, H2OCoxPH
from pysparkling.ml import H2ODeepLearning, H2OKMeans, H2OIsolationForest
from pysparkling.ml import H2OAutoEncoder, H2OPCA

def testGBMParameters(prostateDataset):
    features = ['AGE', 'RACE', 'DPROS', 'DCAPS', 'PSA']
    algorithm = H2OGBM(seed=1, labelCol="CAPSULE", featuresCols=features, monotoneConstraints={'AGE': 1, 'RACE': -1})
    model = algorithm.fit(prostateDataset)
    ignored=["getMonotoneConstraints"]  # Will be fixed by SW-2572
    compareParameterValues(algorithm, model, ignored)


def testDRFParameters(prostateDataset):
    features = ['AGE', 'RACE', 'DPROS', 'DCAPS', 'PSA']
    algorithm = H2ODRF(seed=1, labelCol="CAPSULE", featuresCols=features, classSamplingFactors=[.2, .8, 1])
    model = algorithm.fit(prostateDataset)
    compareParameterValues(algorithm, model)


def testXGBoostParameters(prostateDataset):
    features = ['AGE', 'RACE', 'DPROS', 'DCAPS', 'PSA']
    algorithm = H2OXGBoost(seed=1, labelCol="CAPSULE", featuresCols=features,
                           monotoneConstraints={'AGE': 1, 'RACE': -1},
                           interactionConstraints=[['AGE', 'RACE', 'DPROS'], ['DCAPS', 'PSA']])
    model = algorithm.fit(prostateDataset)
    ignored=["getInteractionConstraints", "getMonotoneConstraints"]  # Will be fixed by SW-2573 and SW-2572
    compareParameterValues(algorithm, model, ignored)


def testGLMParameters(prostateDataset):
    features = ['AGE', 'RACE', 'DPROS', 'DCAPS', 'PSA']
    algorithm = H2OGLM(seed=1, labelCol="CAPSULE", alphaValue=[0.5], lambdaValue=[0.5], maxIterations=30,
                       objectiveEpsilon=0.001, gradientEpsilon=0.001, objReg=0.001, maxActivePredictors=3000,
                       lambdaMinRatio=0.001, featuresCols=features)
    model = algorithm.fit(prostateDataset)
    compareParameterValues(algorithm, model)


def testGAMParameters(prostateDataset):
    features = ['AGE', 'RACE', 'DPROS', 'DCAPS', 'PSA']
    algorithm = H2OGAM(seed=1, labelCol="CAPSULE", gamCols=[["PSA"], ["AGE"]], numKnots=[5, 5], lambdaValue=[0.5],
                       featuresCols=features, bs=[1, 1], scale=[0.5, 0.5])
    model = algorithm.fit(prostateDataset)
    compareParameterValues(algorithm, model, ["getFeaturesCols"])


def testDeepLearningParameters(prostateDataset):
    features = ['AGE', 'RACE', 'DPROS', 'DCAPS', 'PSA']
    algorithm = H2ODeepLearning(seed=1, labelCol="CAPSULE", featuresCols=features, reproducible=True)
    model = algorithm.fit(prostateDataset)
    compareParameterValues(algorithm, model)


def testKmeansParameters(prostateDataset):
    features = ['AGE', 'RACE', 'DPROS', 'DCAPS', 'PSA']
    algorithm = H2OKMeans(seed=1, featuresCols=features)
    model = algorithm.fit(prostateDataset)
    compareParameterValues(algorithm, model)


def testIsolationForestParameters(prostateDataset):
    features = ['AGE', 'RACE', 'DPROS', 'DCAPS', 'PSA']
    algorithm = H2OIsolationForest(seed=1, sampleRate=0.5, featuresCols=features)
    model = algorithm.fit(prostateDataset)
    compareParameterValues(algorithm, model)


def testCoxPHParameters(heartDataset):
    features = ['age', 'year', 'surgery', 'transplant', 'start', 'stop']
    algorithm = H2OCoxPH(labelCol="event", featuresCols=features, startCol='start', stopCol='stop')
    model = algorithm.fit(heartDataset)
    compareParameterValues(algorithm, model)


def testAutoEncoderParameters(prostateDataset):
    features = ["RACE", "DPROS", "DCAPS"]
    algorithm = H2OAutoEncoder(seed=1, inputCols=features, reproducible=True, hidden=[3,])
    model = algorithm.fit(prostateDataset)
    compareParameterValues(algorithm, model)


def testPCAParameters(prostateDataset):
    features = ['AGE', 'RACE', 'DPROS', 'DCAPS', 'PSA']
    algorithm = H2OPCA(seed=1, inputCols=features, k=3)
    model = algorithm.fit(prostateDataset)
    ignored = ["getPcaImpl"]  # PUBDEV-8217: Value of pca_impl isn't propagated to MOJO models
    compareParameterValues(algorithm, model, ignored)


def compareParameterValues(algorithm, model, ignored=[]):
    algorithmMethods = dir(algorithm)

    def isMethodRelevant(method):
        return method.startswith("get") and \
            getattr(model, method).__code__.co_argcount == 1 and \
            method in algorithmMethods and \
            method not in ignored

    methods = filter(isMethodRelevant, dir(model))

    for method in methods:
        modelValue = getattr(model, method)()
        algorithmValue = getattr(algorithm, method)()
        assert(valuesAreEqual(algorithmValue, modelValue))


def valuesAreEqual(algorithmValue, modelValue):
    if algorithmValue == "AUTO":
        return True
    elif algorithmValue == "auto":
        return True
    elif algorithmValue == "family_default":
        return True
    elif algorithmValue == {} and modelValue is None:
        return True
    else:
        return algorithmValue == modelValue


