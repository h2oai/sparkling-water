from py_sparkling.ml.util import getDoubleArrayFromIntArray


def getDoubleArrayArrayFromIntArrayArray(array):
    if array is None:
        return None
    else:
        return [getDoubleArrayFromIntArray(arr) for arr in array]
