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

import h2o
import sys
import warnings
from h2o.frame import H2OFrame
from h2o.utils.typechecks import assert_is_type, Enum
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *

from ai.h2o.sparkling.FrameConversions import FrameConversions as fc
from ai.h2o.sparkling.Initializer import Initializer
from ai.h2o.sparkling.H2OConf import H2OConf

def _is_of_simple_type(rdd):
    if not isinstance(rdd, RDD):
        raise ValueError('rdd is not of type pyspark.rdd.RDD')

    # Python 3.6 does not contain type long
    # this code ensures we are compatible with both, python 2.7 and python 3.6
    if sys.version_info > (3,):
        type_checks = (str, int, bool, float)
    else:
        type_checks = (str, int, bool, long, float)

    if not rdd.isEmpty() and isinstance(rdd.first(), type_checks):
        return True
    else:
        return False


def _get_first(rdd):
    if rdd.isEmpty():
        raise ValueError('rdd is empty')

    return rdd.first()


class H2OContext(object):

    def __init__(self, spark_session):
        """
         This constructor is used just to initialize the environment. It does not start H2OContext.
         To start H2OContext use one of the getOrCreate methods. This constructor is internally used in those methods
        """
        try:
            self.__do_init(spark_session)
            Initializer.load_sparkling_jar()
        except:
            raise

    def __do_init(self, spark_session):
        self._spark_session = spark_session
        self._sc = self._spark_session._sc
        self._sql_context = self._spark_session._wrapped
        self._jsql_context = self._spark_session._jwrapped
        self._jspark_session = self._spark_session._jsparkSession
        self._jvm = self._spark_session._jvm

    def __h2o_connect(h2o_context, **kwargs):
        if "https" in kwargs:
            warnings.warn("https argument is automatically set up and the specified value will be ignored.")
        schema = h2o_context._jhc.h2oContext().getConf().getScheme()
        kwargs["https"] = False

        conf = h2o_context._conf
        if conf.userName() and conf.password():
            kwargs["auth"] = (conf.userName(), conf.password())
        if schema == "https":
            kwargs["https"] = True
        if h2o_context._conf.contextPath() is not None:
            url = "{}://{}:{}/{}".format(schema, h2o_context._client_ip, h2o_context._client_port, h2o_context._conf.context_path())
            return h2o.connect(url=url, **kwargs)
        else:
            return h2o.connect(ip=h2o_context._client_ip, port=h2o_context._client_port, **kwargs)


    @staticmethod
    def getOrCreate(spark, conf=None, verbose=True, **kwargs):
        """
        Get existing or create new H2OContext based on provided H2O configuration. If the conf parameter is set then
        configuration from it is used. Otherwise the configuration properties passed to Sparkling Water are used.
        If the values are not found the default values are used in most of the cases. The default cluster mode
        is internal, ie. spark.ext.h2o.external.cluster.mode=false

        :param spark: Spark Context or Spark Session
        :param conf: H2O configuration as instance of H2OConf
        :param verbose; True if verbose H2O output
        :param kwargs:  additional parameters which are passed to h2o_connect_hook
        :return:  instance of H2OContext
        """

        # Get spark session
        spark_session = spark
        if isinstance(spark, SparkContext):
            warnings.warn("Method H2OContext.getOrCreate with argument of type SparkContext is deprecated and " +
                          "parameter of type SparkSession is preferred.")
            spark_session = SparkSession.builder.getOrCreate()
        # Get H2OConf
        if conf is not None:
            selected_conf = conf
        else:
            selected_conf = H2OConf(spark_session)
        if "auth" in kwargs:
            warnings.warn("Providing authentication via auth field on H2OContext is deprecated. "
                          "Please use setUserName and setPassword setters on H2OConf object.")
            selected_conf.setUserName(kwargs["auth"][0])
            selected_conf.setPassword(kwargs["auth"][1])
            del kwargs["auth"]

        h2o_context = H2OContext(spark_session)

        jvm = h2o_context._jvm  # JVM
        jspark_session = h2o_context._jspark_session  # Java Spark Session

        # Create backing Java H2OContext
        jhc = jvm.org.apache.spark.h2o.JavaH2OContext.getOrCreate(jspark_session, selected_conf._jconf)
        h2o_context._jhc = jhc
        h2o_context._conf = selected_conf
        h2o_context._client_ip = jhc.h2oLocalClientIp()
        h2o_context._client_port = jhc.h2oLocalClientPort()

        # Create H2O REST API client
        if not h2o_context.__isClientConnected():
            h2o_context.__h2o_connect(verbose=verbose, **kwargs)

        h2o_context.__setClientConnected()

        if verbose:
            print(h2o_context)

        return h2o_context

    def __isStopped(self):
        hc = self._jhc.h2oContext()
        field = hc.getClass().getSuperclass().getDeclaredField("stopped")
        field.setAccessible(True)
        return field.get(hc)

    def __isClientConnected(self):
        hc = self._jhc.h2oContext()
        field = self.__getClientConnectedField()
        return field.get(hc)

    def __setClientConnected(self):
        hc = self._jhc.h2oContext()
        field = self.__getClientConnectedField()
        field.set(hc, True)

    def __getClientConnectedField(self):
        hc = self._jhc.h2oContext()
        field = hc.getClass().getSuperclass().getDeclaredField("clientConnected")
        field.setAccessible(True)
        return field

    def stop(self):
        h2o.connection().close()
        hc = self._jhc.h2oContext()
        scalaStopMethod = getattr(hc, "org$apache$spark$h2o$H2OContext$$stop")
        scalaStopMethod(False, False, False) # stopSpark = False, stopJVM = False, inShutdownHook = False

        if self._conf.get("spark.ext.h2o.rest.api.based.client", "false") == "false":
            sys.exit()

    def download_h2o_logs(self, destination, container = "ZIP"):
        assert_is_type(container, Enum("ZIP", "LOG"))
        return self._jhc.h2oContext().downloadH2OLogs(destination, container)

    def __str__(self):
        if self.__isClientConnected() and not self.__isStopped():
            return self._jhc.toString()
        else:
            return "H2OContext has been stopped or hasn't been created. Call H2OContext.getOrCreate(spark) or " \
                   "H2OContext.getOrCreate(spark, conf) to create a new one."

    def __repr__(self):
        self.show()
        return ""

    def show(self):
        print(self)

    def get_conf(self):
        return self._conf

    def asSparkFrame(self, h2oFrame, copyMetadata=True):
        """
        Transforms given H2OFrame to Spark DataFrame

        Parameters
        ----------
         h2oFrame : H2OFrame
         copyMetadata: Bool = True

        Returns
        -------
         Spark DataFrame
        """

        if isinstance(h2oFrame, H2OFrame):
            frame_id = h2oFrame.frame_id
            jdf = self._jhc.asDataFrame(frame_id, copyMetadata)
            sqlContext = SparkSession.builder.getOrCreate()._wrapped
            df = DataFrame(jdf, sqlContext)
            # Attach h2o_frame to dataframe which forces python not to delete the frame when we leave the scope of this
            # method.
            # Without this, after leaving this method python would garbage collect the frame since it's not used
            # anywhere and spark. when executing any action on this dataframe, will fail since the frame
            # would be missing.
            df._h2o_frame = h2oFrame
            return df

    def as_spark_frame(self, h2o_frame, copy_metadata=True):
        warnings.warn("Method 'as_spark_frame' is deprecated and will be removed in release 3.30. Please use method 'asSparkFrame' instead!")
        return self.asSparkFrame(h2o_frame, copy_metadata)

    def asH2OFrame(self, sparkFrame, h2oFrameName=None, fullCols=-1):
        """
        Transforms given Spark RDD or DataFrame to H2OFrame.

        Parameters
        ----------
          sparkFrame : Spark RDD or DataFrame
          h2oFrameName : Optional name for resulting H2OFrame
          fullCols : number of first n columns which are sent to the client together with the data

        Returns
        -------
          H2OFrame which contains data of original input Spark data structure
        """
        if isinstance(sparkFrame, DataFrame):
            return fc._as_h2o_frame_from_dataframe(self, sparkFrame, h2oFrameName, fullCols)
        elif isinstance(sparkFrame, RDD) and sparkFrame.isEmpty():
            schema = StructType([])
            empty = self._spark_session.createDataFrame(self._spark_session.sparkContext.emptyRDD(), schema)
            return fc._as_h2o_frame_from_dataframe(self, empty, h2oFrameName, fullCols)
        elif isinstance(sparkFrame, RDD):
            # First check if the type T in RDD[T] is one of the python "primitive" types
            # String, Boolean, Int and Double (Python Long is converted to java.lang.BigInteger)
            if _is_of_simple_type(sparkFrame):
                first = _get_first(sparkFrame)
                # Make this code compatible with python 3.6 and python 2.7
                global long
                if sys.version_info > (3,):
                    long = int

                if isinstance(first, str):
                    return fc._as_h2o_frame_from_RDD_String(self, sparkFrame, h2oFrameName, fullCols)
                elif isinstance(first, bool):
                    return fc._as_h2o_frame_from_RDD_Bool(self, sparkFrame, h2oFrameName, fullCols)
                elif (isinstance(sparkFrame.min(), int) and isinstance(sparkFrame.max(), int)) or (isinstance(sparkFrame.min(), long) and isinstance(sparkFrame.max(), long)):
                    if sparkFrame.min() >= self._jvm.Integer.MIN_VALUE and sparkFrame.max() <= self._jvm.Integer.MAX_VALUE:
                        return fc._as_h2o_frame_from_RDD_Int(self, sparkFrame, h2oFrameName, fullCols)
                    elif sparkFrame.min() >= self._jvm.Long.MIN_VALUE and sparkFrame.max() <= self._jvm.Long.MAX_VALUE:
                        return fc._as_h2o_frame_from_RDD_Long(self, sparkFrame, h2oFrameName, fullCols)
                    else:
                        raise ValueError('Numbers in RDD Too Big')
                elif isinstance(first, float):
                    return fc._as_h2o_frame_from_RDD_Float(self, sparkFrame, h2oFrameName, fullCols)
            else:
                return fc._as_h2o_frame_from_complex_type(self, sparkFrame, h2oFrameName, fullCols)
        else:
            raise ValueError('The asH2OFrame method expects Spark DataFrame or RDD as the input only!')

    def as_h2o_frame(self, dataframe, framename=None, full_cols=-1):
        warnings.warn("Method 'as_h2o_frame' is deprecated and will be removed in release 3.30. Please use method 'asH2OFrame' instead!")
        return self.asH2OFrame(dataframe, framename, full_cols)
