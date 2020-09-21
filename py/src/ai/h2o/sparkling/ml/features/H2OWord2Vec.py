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

from ai.h2o.sparkling import Initializer
from ai.h2o.sparkling.ml.H2OStageBase import H2OStageBase
from ai.h2o.sparkling.ml.Utils import Utils
from ai.h2o.sparkling.ml.models import H2OMOJOModel
from ai.h2o.sparkling.ml.params.H2OWord2VecParams import H2OWord2VecParams
from pyspark import keyword_only
from pyspark.ml.wrapper import JavaEstimator
from pyspark.ml.param.shared import HasInputCol, HasOutputCol

class H2OWord2Vec(HasInputCol, HasOutputCol, H2OWord2VecParams, H2OStageBase, JavaEstimator):

    @keyword_only
    def __init__(self,
                 inputCol=None,
                 outputCol="Word2Vec_output",
                 vecSize=5,
                 windowWize=100,
                 sentSampleRate=0.001,
                 normModel="HSM",
                 epochs=5,
                 minWordFreq=5,
                 initLearningRate=0.025,
                 wordModel="SkipGram",
                 modelId=None,
                 maxRuntimeSecs=0.0,
                 exportCheckpointsDir=None):
        Initializer.load_sparkling_jar()
        super(H2OWord2Vec, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.features.H2OWord2Vec", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)

    def setInputCol(self, value):
        return self._set(inputCol=value)

    def setOutputCol(self, value):
        return self._set(outputCol=value)
