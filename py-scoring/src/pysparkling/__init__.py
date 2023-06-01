import warnings

from pysparkling.initializer import Initializer


def custom_formatwarning(msg, *args, **kwargs):
    # ignore everything except the message
    return str(msg) + '\n'


warnings.formatwarning = custom_formatwarning

__version__ = Initializer.getVersion()

Initializer.load_sparkling_jar()
