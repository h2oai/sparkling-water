from pyspark.ml.util import JavaMLReader

class H2OJavaMLReader(JavaMLReader):

    @classmethod
    def _java_loader_class(cls, clazz):
        java_package = ".".join(clazz.__module__.split(".")[0:-1])
        return java_package + "." + clazz.__name__
