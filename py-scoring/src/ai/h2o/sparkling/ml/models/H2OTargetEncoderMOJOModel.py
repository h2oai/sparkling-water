from ai.h2o.sparkling.ml.params.H2OTargetEncoderMOJOParams import H2OTargetEncoderMOJOParams
from pyspark.ml.util import JavaMLWritable
from ai.h2o.sparkling.ml.util.H2OJavaMLReadable import H2OJavaMLReadable
from pyspark.ml.wrapper import JavaModel


class H2OTargetEncoderMOJOModel(H2OTargetEncoderMOJOParams, JavaModel, JavaMLWritable, H2OJavaMLReadable):
    pass
