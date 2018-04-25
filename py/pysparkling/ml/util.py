from pyspark.ml.util import MLReadable, JavaMLReadable, JavaMLWritable, JavaMLReader
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaParams

class JavaH2OMLReadable(MLReadable):
    """
    Special version of JavaMLReadable to be able to load pipelines exported together with H2O pipeline stages
    """
    def __init__(self):
        super(JavaH2OMLReadable, self).__init__()

    """
    (Private) Mixin for instances that provide JavaMLReader.
    """

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return JavaH2OMLReader(cls)


class JavaH2OMLReader(JavaMLReader):

    def __init__(self, clazz):
        super(JavaH2OMLReader, self).__init__(clazz)
        self._clazz = clazz
        self._jread = self._load_java_obj(clazz).read()

    @classmethod
    def _java_loader_class(cls, clazz):
        """
        We need to return the path which is specific to pysparkling structure
        """
        return "org.apache.spark." + clazz.__module__.replace("pysparkling.ml", "ml.h2o") + "." + clazz.__name__

class H2OAlgo(JavaEstimator, JavaH2OMLReadable, JavaMLWritable):
    """
    Wrapper around JavaParams overriding _from_java method to be aware of pysparkling specific structure
    """
    @staticmethod
    def _from_java(java_stage):
        """
        Given a Java object, create and return a Python wrapper of it.
        Used for ML persistence.

        Meta-algorithms such as Pipeline should override this method as a classmethod.
        """
        def __get_class(clazz):
            """
            Loads Python class from its name.
            """
            parts = clazz.replace("h2o.", "").split('.') # here we remove h2o. from the name
            module = ".".join(parts[:-1])
            m = __import__(module)
            for comp in parts[1:]:
                m = getattr(m, comp)
            return m
        stage_name = java_stage.getClass().getName().replace("org.apache.spark", "pysparkling")
        # Generate a default new instance from the stage_name class.
        py_type = __get_class(stage_name)
        if issubclass(py_type, JavaParams):
            # Load information from java_stage to the instance.
            py_stage = py_type()
            py_stage._java_obj = java_stage

            # SPARK-10931: Temporary fix so that persisted models would own params from Estimator
            if issubclass(py_type, JavaModel):
                py_stage._create_params_from_java()

            py_stage._resetUid(java_stage.uid())
            py_stage._transfer_params_from_java()
        elif hasattr(py_type, "_from_java"):
            py_stage = py_type._from_java(java_stage)
        else:
            raise NotImplementedError("This Java stage cannot be loaded into Python currently: %r"
                                      % stage_name)
        return py_stage