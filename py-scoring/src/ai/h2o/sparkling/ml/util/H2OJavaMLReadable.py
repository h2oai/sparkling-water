from pyspark.ml.util import JavaMLReadable
from ai.h2o.sparkling.ml.util.H2OJavaMLReader import H2OJavaMLReader

class H2OJavaMLReadable(JavaMLReadable):

    @classmethod
    def read(cls):
        return H2OJavaMLReader(cls)
