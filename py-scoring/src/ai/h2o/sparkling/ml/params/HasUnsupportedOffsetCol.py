
class HasUnsupportedOffsetCol:

    def getOffsetCol(self):
        return None

    def setOffsetCol(self, value):
        raise NotImplementedError("The parameter 'offsetCol' is not yet supported.")
