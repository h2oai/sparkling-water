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

from ai.h2o.sparkling.ml.params.H2OTargetEncoderMOJOParams import H2OTargetEncoderMOJOParams
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.wrapper import JavaModel
from pyspark.sql import DataFrame
import inspect


class H2OTargetEncoderModel(H2OTargetEncoderMOJOParams, JavaModel, JavaMLWritable):

    def transform(self, dataset):
        callerFrame = inspect.stack()[1]
        inTrainingMode = (callerFrame[3] == '_fit') & callerFrame[1].endswith('pyspark/ml/pipeline.py')
        if inTrainingMode:
            return self.transformTrainingDataset(dataset)
        else:
            return super(H2OTargetEncoderModel, self).transform(dataset)

    def transformTrainingDataset(self, dataset):
        self._transfer_params_to_java()
        return DataFrame(self._java_obj.transformTrainingDataset(dataset._jdf), dataset.sql_ctx)
