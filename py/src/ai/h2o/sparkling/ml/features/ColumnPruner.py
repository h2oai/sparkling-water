from ai.h2o.sparkling.Initializer import Initializer
from ai.h2o.sparkling.ml.H2OStageBase import H2OStageBase
from ai.h2o.sparkling.ml.Utils import Utils
from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark import keyword_only
from pyspark.ml.param.shared import *
from pyspark.ml.wrapper import JavaTransformer


class ColumnPruner(H2OStageBase, JavaTransformer):
    keep = Param(
        Params._dummy(),
        "keep",
        "keep the specified columns in the frame",
        H2OTypeConverters.toBoolean())

    columns = Param(
        Params._dummy(),
        "columns",
        "specified columns",
        H2OTypeConverters.toListString())

    @keyword_only
    def __init__(self,
                 keep=False,
                 columns=[]):
        Initializer.load_sparkling_jar()
        super(ColumnPruner, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.features.ColumnPruner", self.uid)
        self._setDefaultValuesFromJava()
        kwargs = Utils.getInputKwargs(self)
        self._set(**kwargs)

    def setKeep(self, value):
        return self._set(keep=value)

    def setColumns(self, value):
        return self._set(columns=value)

    def getKeep(self):
        return self.getOrDefault(self.keep)

    def getColumns(self):
        return self.getOrDefault(self.columns)
