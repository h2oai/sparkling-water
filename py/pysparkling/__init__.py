#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pyspark
import warnings
import re

from pysparkling.initializer import Initializer
from pysparkling.initializer import VersionComponents

__version__ = Initializer.getVersion()
pySparklingVersionComponents = VersionComponents.parseFromVersion(__version__)
pySparkMinorVersion = re.search(r"^(\d+\.\d+)\.\d+$", pyspark.__version__).group(1)

def custom_formatwarning(msg, *args, **kwargs):
    # ignore everything except the message
    return str(msg) + '\n'

warnings.formatwarning = custom_formatwarning


if not (pySparkMinorVersion == pySparklingVersionComponents.sparkVersion):
    warnings.warn("""
    You are using PySparkling for Spark {}, but your PySpark is of version {}.
    Please make sure Spark and PySparkling versions are compatible.""".format(
        pySparklingVersionComponents.sparkVersion, pySparkMinorVersion))


# set imports from this project which will be available when the module is imported
from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf

Initializer.check_different_h2o()
# set what is meant by * packages in statement from foo import *
__all__ = ["H2OContext", "H2OConf"]

Initializer.load_sparkling_jar()
