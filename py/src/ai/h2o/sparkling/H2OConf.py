from ai.h2o.sparkling.ExternalBackendConf import ExternalBackendConf
from ai.h2o.sparkling.Initializer import Initializer
from ai.h2o.sparkling.InternalBackendConf import InternalBackendConf
from ai.h2o.sparkling.SharedBackendConf import SharedBackendConf
from pyspark.sql import SparkSession


class H2OConf(SharedBackendConf, InternalBackendConf, ExternalBackendConf):
    def __init__(self):
        try:
            Initializer.load_sparkling_jar()
            _jvm = SparkSession._instantiatedSession.sparkContext._jvm
            self._jconf = _jvm.ai.h2o.sparkling.H2OConf()
        except:
            raise
