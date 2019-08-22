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

import os
import sys
import warnings
import zipfile
from codecs import open
from os import path
from pyspark import SparkContext

"""
This class is used to load sparkling water JAR into spark environment - driver and executors.
The JAR is loaded when we call or import any of the publicly available PySparkling class.

This ensure the required java classes are available in Spark without the need of configuring JAR file using
--jars option.
"""


class Initializer(object):
    # Flag to inform us whether sparkling jar has been already loaded or not.
    # We don't want to load it more than once.
    __sparklingWaterJarLoaded = False
    __extracted_jar_dir = None

    @staticmethod
    def load_sparkling_jar():
        sc = SparkContext._active_spark_context
        if Initializer.__sparklingWaterJarLoaded is not True and sc is not None:
            jvm = sc._jvm
            stream = jvm.Thread.currentThread().getContextClassLoader().getResourceAsStream("sw.version")
            if stream is None:
                sys.path.append(".")
                Initializer.__add_sparkling_jar_to_spark(sc)
            else:
                otherVersion = jvm.scala.io.Source.fromInputStream(stream, "UTF-8").mkString()
                currentVersion = Initializer.getVersion()
                if otherVersion != currentVersion:
                    raise Exception("JAR file for Sparkling Water {} is already attached to the cluster, but you " \
                                    "are starting PySparkling for {}. Either remove the attached JAR of the different " \
                                    "version or use PySparkling of the same version as the attached JAR.".format(
                        otherVersion,
                        currentVersion))
            Initializer.__sparklingWaterJarLoaded = True

    @staticmethod
    def __add_sparkling_jar_to_spark(sc):
        jvm = sc._jvm
        # Add Sparkling water assembly JAR to driver
        sw_jar_file = Initializer.__get_sw_jar(sc)

        # SW-593 - adding an extra / to fix a windows shell issue creating malform url
        if not sw_jar_file.startswith('/'):
            sw_jar_file = '/' + sw_jar_file

        url = jvm.java.net.URL("file://{0}".format(sw_jar_file))

        Initializer.__add_url_to_classloader(jvm, url)

        # Add Sparkling Water Assembly JAR to Spark's file server so executors can fetch it
        # when they need to use the dependency.
        sc._jsc.addJar(sw_jar_file)

    @staticmethod
    def __extracted_jar_path(sc):

        if Initializer.__extracted_jar_dir is None:
            zip_file = Initializer.__get_pysparkling_zip_path()
            Initializer.__extracted_jar_dir = sc._temp_dir
            import zipfile
            with zipfile.ZipFile(zip_file) as fzip:
                fzip.extract('sparkling_water/sparkling_water_assembly.jar', path=Initializer.__extracted_jar_dir)

        return os.path.abspath(
            "{}/sparkling_water/sparkling_water_assembly.jar".format(Initializer.__extracted_jar_dir))

    @staticmethod
    def __get_pysparkling_zip_path():
        import sparkling_water
        sw_pkg_file = sparkling_water.__file__
        return sw_pkg_file[:-len('/sparkling_water/__init__.py')]

    @staticmethod
    def check_different_h2o():
        import subprocess
        try:
            from subprocess import DEVNULL  # py3k
        except ImportError:
            DEVNULL = open(os.devnull, 'wb')

        try:
            import h2o
            sw_h2o_version = h2o.__version__
            zip_file_name = os.path.basename(Initializer.__get_pysparkling_zip_path())
            path_without_sw = [i for i in sys.path if os.path.basename(i) != zip_file_name]
            command_sys_path = "import sys; sys.path = " + str(path_without_sw).replace("'", "\"") + ";"
            command_import_h2o = "import h2o; print(h2o.__version__)"
            full_command = "python -c '" + command_sys_path + command_import_h2o + "'"
            previous_version = subprocess\
                .check_output(full_command, shell=True, stderr=DEVNULL)\
                .decode('utf-8')\
                .replace("\n", "")
            if not previous_version == sw_h2o_version and previous_version is not "":
                warnings.warn("PySparkling is using internally bundled H2O of version {}, but H2O"
                              " installed in the python environment is of version {}."
                              .format(sw_h2o_version, previous_version))
        except subprocess.CalledProcessError:
            pass

    @staticmethod
    def __get_sw_jar(sc):
        import sparkling_water
        sw_pkg_file = sparkling_water.__file__
        # Extract jar file from zip
        if '.zip' in sw_pkg_file:
            return Initializer.__extracted_jar_path(sc)
        else:
            from pkg_resources import resource_filename
            return os.path.abspath(resource_filename("sparkling_water", 'sparkling_water_assembly.jar'))

    @staticmethod
    def __get_logger(jvm):
        return jvm.org.apache.log4j.LogManager.getLogger("org")

    @staticmethod
    def __add_url_to_classloader(jvm, url):
        cl = jvm.Thread.currentThread().getContextClassLoader()
        logger = Initializer.__get_logger(jvm)
        while cl:
            methods = [m.getName() for m in cl.getClass().getDeclaredMethods()]
            if "addURL" in methods:
                cl.addURL(url)
                logger.debug("Adding {} to classloader '{}'".format(url.toString(), cl.toString()))
            else:
                logger.debug("Skipping classloader '{}'".format(cl.toString()))
            cl = cl.getParent()

    @staticmethod
    def getVersion():
        here = path.abspath(path.dirname(__file__))
        if '.zip' in here:
            with zipfile.ZipFile(here[:-len("ai/h2o/sparkling/")], 'r') as archive:
                return archive.read('ai/h2o/sparkling/version.txt').decode('utf-8').strip()
        else:
            with open(path.join(here, 'version.txt'), encoding='utf-8') as f:
                return f.read().strip()
