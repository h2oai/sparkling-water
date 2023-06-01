from pyspark.ml.util import JavaMLWritable, JavaMLReadable


class H2OStageBase(JavaMLReadable, JavaMLWritable):

    # Set default values directly from Scala so we don't have to duplicate it on PySpark side
    def _setDefaultValuesFromJava(self):
        for paramPair in self._java_obj.extractParamMap().toList():
            paramName = paramPair.param().name()
            paramValue = self._java_obj.getDefault(paramPair.param()).get()
            param = getattr(self, paramName)
            self._defaultParamMap[param] = param.typeConverter(paramValue)

        return self

    # Override of _set method
    # Spark's _set method skips parameters with None values, but we want to validate them as well
    def _set(self, **kwargs):
        """
        Sets user-supplied params.
        """
        for param, value in kwargs.items():
            p = getattr(self, param)
            try:
                value = p.typeConverter(value)
            except TypeError as e:
                raise TypeError('Invalid param value given for param "%s". %s' % (p.name, e))
            self._paramMap[p] = value
        return self
