from pkg_resources import resource_filename
import sys
"""
This class is used to load sparkling water JAR into spark environment. It is called when H2OConf or H2OContext is used
to ensure the required java classes are available in Spark.
"""
class Initializer(object):

    # Flag to inform us whether sparkling jar has been already loaded or not. We don't want to load it more than once.
    __sparkling_jar_loaded = False

    @staticmethod
    def load_sparkling_jar(spark_context):
        if not Initializer.__sparkling_jar_loaded:
            sys.path.append(".")
            Initializer.__add_sparkling_jar_to_spark(spark_context)
            Initializer.__sparkling_jar_loaded = True

    @staticmethod
    def __add_sparkling_jar_to_spark(sc):
        # Add Sparkling water assembly JAR to driver
        url = sc._jvm.java.net.URL("file://"+Initializer.__get_sw_jar())
        # Assuming that context class loader is always instance of URLClassLoader ( which should be always true)
        cl = sc._jvm.Thread.currentThread().getContextClassLoader()

        # explicitly check if we run on databricks cloud since there we must add the jar to the parent of context class loader
        if cl.getClass().getName()=='com.databricks.backend.daemon.driver.DriverLocal$DriverLocalClassLoader':
            cl.getParent().getParent().addURL(url)
        else:
            cl.addURL(url)

        # Add Sparkling water assembly JAR to executors
        sc._jsc.addJar(Initializer.__get_sw_jar())

    @staticmethod
    def __get_sw_jar():
        return resource_filename("sparkling_water", 'sparkling_water_assembly.jar')

