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

here = path.abspath(path.dirname(__file__))

__version__ = "0.0.local"
if '.zip' in here:
    with zipfile.ZipFile(path.dirname(here), 'r') as archive:
        __version__ = archive.read('pysparkling/version.txt').decode('utf-8').strip()
else:
    with open(path.join(here, 'version.txt'), encoding='utf-8') as f:
        __version__ = f.read().strip()

# set imports from this project which will be available when the module is imported
from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf

# set what is meant by * packages in statement from foo import *
__all__ = ["H2OContext", "H2OConf"]
