# encoding: utf-8
# module pySparkling
# from (pysparkling)
"""
pySparkling - The Sparkling-Water Python Package
=====================
"""

from .features import ColumnPruner
from .algos import H2OGBM, H2ODeepLearning, H2OAutoML, H2OXGBoost, H2OGLM, H2OGridSearch
from .models import H2OMOJOModel, H2OMOJOPipelineModel, H2OMOJOSettings
from ai.h2o.sparkling.ml import H2OKMeans, H2OTargetEncoder

# set what is meant by * packages in statement from foo import *
__all__ = ["ColumnPruner", "H2OGBM", "H2ODeepLearning", "H2OAutoML", "H2OXGBoost", "H2OGLM", "H2OMOJOModel", "H2OMOJOPipelineModel", "H2OGridSearch", "H2OMOJOSettings", "H2OKMeans", "H2OTargetEncoder"]

from pysparkling.initializer import Initializer

# Load sparkling water jar only if Spark is already running
sc = Initializer.active_spark_context()
if sc is not None:
    Initializer.load_sparkling_jar(sc)
