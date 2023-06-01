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
