from pysparkling.ml import *


def getParamPairs(algoClass):
    # Create dummy instance which we use just to obtain default values
    params = algoClass()._defaultParamMap
    kwargs = {}
    for param in list(params):
        kwargs[param.name] = params[param]
    return kwargs


def assertParamsViaConstructor(algoName, skip=[]):
    AlgoClass = globals()[algoName]
    kwargs = getParamPairs(AlgoClass)
    instance = AlgoClass(**kwargs)
    for name in kwargs:
        if name not in skip:
            # Assert that the getter is giving the value we passed via constructor
            getter = getattr(instance, "get" + name[:1].upper() + name[1:])
            assert getter() == kwargs[name]


def assertParamsViaSetters(algoName, skip=[]):
    AlgoClass = globals()[algoName]
    kwargs = getParamPairs(AlgoClass)
    instance = AlgoClass(**kwargs)

    for name in kwargs:
        if name not in skip:
            # Assert that the getter is giving the value we passed via setter
            setter = getattr(instance, "set" + name[:1].upper() + name[1:])
            getter = getattr(instance, "get" + name[:1].upper() + name[1:])
            setter(kwargs[name])
            assert getter() == kwargs[name]
