import sys
import os
from pyspark.context import SparkContext
import warnings

"""
This class is used to load sparkling water JAR into spark environment.
It is called when H2OConf or H2OContext is used
to ensure the required java classes are available in Spark.
"""


class Initializer(object):
    # Flag to inform us whether sparkling jar has been already loaded or not.
    # We don't want to load it more than once.
    __sparkling_jar_loaded = False
    __extracted_jar_dir = None

    @staticmethod
    def load_sparkling_jar(spark):
        if isinstance(spark, SparkContext):
            sc = spark
        else:  # We have Spark Session
            sc = spark.sparkContext
        if not Initializer.__sparkling_jar_loaded:
            sys.path.append(".")
            Initializer.__add_sparkling_jar_to_spark(sc)
            Initializer.__sparkling_jar_loaded = True

    @staticmethod
    def __add_sparkling_jar_to_spark(sc):
        jsc = sc._jsc
        jvm = sc._jvm
        # Add Sparkling water assembly JAR to driver
        sw_jar_file = Initializer.__get_sw_jar()

        # SW-593 - adding an extra / to fix a windows shell issue creating malform url
        if not sw_jar_file.startswith('/'):
            sw_jar_file = '/' + sw_jar_file

        url = jvm.java.net.URL("file://{0}".format(sw_jar_file))
        # Assuming that context class loader is always instance of URLClassLoader
        # ( which should be always true)
        cl = jvm.Thread.currentThread().getContextClassLoader()

        # Explicitly check if we run on databricks cloud since there we must add the jar to
        # the parent of context class loader
        if cl.getClass().getName() == 'com.databricks.backend.daemon.driver.DriverLocal$DriverLocalClassLoader':
            cl.getParent().getParent().addURL(url)
        else:
            spark_cl = Initializer.__find_spark_cl(cl, 'org.apache.spark.util.MutableURLClassLoader')
            spark_cl.addURL(url)

        # Add Sparkling Water Assembly JAR to Spark's file server so executors can fetch it
        # when they need to use the dependency.
        jsc.addJar(sw_jar_file)

    @staticmethod
    def __extracted_jar_path():

        if Initializer.__extracted_jar_dir is None:
            zip_file = Initializer.__get_pysparkling_zip_path()
            import tempfile
            Initializer.__extracted_jar_dir = tempfile.mkdtemp()
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
            previous_version = subprocess.check_output(full_command, shell=True, stderr=DEVNULL).decode('utf-8').replace("\n", "")
            if not previous_version == sw_h2o_version and previous_version is not "":
                warnings.warn("PySparkling is using internally bundled H2O of version " +
                              str(sw_h2o_version) + ", but H2O installed in the python"
                              " environment is of version " + str(previous_version) + ".")
        except subprocess.CalledProcessError:
            pass

    @staticmethod
    def __get_sw_jar():
        import sparkling_water
        sw_pkg_file = sparkling_water.__file__
        # Extract jar file from zip
        if '.zip' in sw_pkg_file:
            return Initializer.__extracted_jar_path()
        else:
            from pkg_resources import resource_filename
            return os.path.abspath(resource_filename("sparkling_water", 'sparkling_water_assembly.jar'))

    @staticmethod
    def __find_spark_cl(start_cl, cl_name):
        cl = start_cl
        while cl:
            name = cl.getClass().getName()
            if (name == cl_name):
                return cl
            cl = cl.getParent()
        return None
