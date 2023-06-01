import warnings

from pysparkling.initializer import Initializer


def custom_formatwarning(msg, *args, **kwargs):
    # ignore everything except the message
    return str(msg) + '\n'


warnings.formatwarning = custom_formatwarning

__version__ = Initializer.getVersion()

from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf

Initializer.check_different_h2o()
__all__ = ["H2OContext", "H2OConf"]

Initializer.load_sparkling_jar()
