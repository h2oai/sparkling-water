# encoding: utf-8
# module pySparkling
# from (pysparkling)
"""
pySparkling - The Sparkling-Water Python Package
=====================
"""
__version__ = "SUBST_PROJECT_VERSION"

# set imports from this project which will be available when the module is imported
from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf

# set what is meant by * packages in statement from foo import *
__all__ = ["H2OContext", "H2OConf"]
