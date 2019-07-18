# encoding: utf-8
# module pySparkling
# from (pysparkling)
"""
pySparkling - The Sparkling-Water Python Package
=====================
"""
import zipfile
from codecs import open
from os import path
import pyspark
import warnings

here = path.abspath(path.dirname(__file__))

__version__ = "0.0.local"
if '.zip' in here:
    with zipfile.ZipFile(path.dirname(here), 'r') as archive:
        __version__ = archive.read('pysparkling/version.txt').decode('utf-8').strip()
else:
    with open(path.join(here, 'version.txt'), encoding='utf-8') as f:
        __version__ = f.read().strip()

pyspark_version = pyspark.__version__.split(".")
pysparkling_spark_version = __version__.split("-")[1].split(".")

pyspark_major = pyspark_version[0] + "." + pyspark_version[1]
pysparkling_spark_major = pysparkling_spark_version[0] + "." + pysparkling_spark_version[1]

def custom_formatwarning(msg, *args, **kwargs):
    # ignore everything except the message
    return str(msg) + '\n'

warnings.formatwarning = custom_formatwarning


if not (pyspark_major == pysparkling_spark_major):
    warnings.warn("""
    You are using PySparkling for Spark {pysparkling_spark_major}, but your PySpark is of
    version {pyspark_major}. Please make sure Spark and PySparkling versions are compatible. """.format(pysparkling_spark_major=pysparkling_spark_major, pyspark_major=pyspark_major))


# set imports from this project which will be available when the module is imported
from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf
from pysparkling.initializer import Initializer

Initializer.check_different_h2o()
# set what is meant by * packages in statement from foo import *
__all__ = ["H2OContext", "H2OConf"]

# Load sparkling water jar only if Spark is already running
sc = Initializer.active_spark_context()
if sc is not None:
    Initializer.load_sparkling_jar(sc)
