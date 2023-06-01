class HasOutputColOnMOJO:

    def getOutputCol(self):
        return self._java_obj.getOutputCol()

    def setOutputCol(self, value):
        self._java_obj.setOutputCol(value)
        return self
