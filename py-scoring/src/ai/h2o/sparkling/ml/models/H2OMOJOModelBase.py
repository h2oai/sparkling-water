from pyspark.ml.param import *
from pyspark.ml.util import JavaMLWritable
from pyspark.ml.wrapper import JavaModel
from ai.h2o.sparkling.ml.util.H2OJavaMLReadable import H2OJavaMLReadable
from ai.h2o.sparkling.ml.params.HasFeatureTypesOnMOJO import HasFeatureTypesOnMOJO

class H2OMOJOModelBase(JavaModel, JavaMLWritable, H2OJavaMLReadable, HasFeatureTypesOnMOJO):

    # Overriding the method to avoid changes on the companion Java object
    def _transfer_params_to_java(self):
        pass

    ##
    # Getters
    ##
    def getConvertUnknownCategoricalLevelsToNa(self):
        return self._java_obj.getConvertUnknownCategoricalLevelsToNa()

    def getConvertInvalidNumbersToNa(self):
        return self._java_obj.getConvertInvalidNumbersToNa()
