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
from pyspark.ml.util import _jvm
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql import SparkSession

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

    __isConnected = False
    def __init__(self):
        """
         This constructor is used just to initialize the environment. It does not start H2OContext.
         To start H2OContext use one of the getOrCreate methods. This constructor is internally used in those methods
        """
        try:
            Initializer.load_sparkling_jar()
        except:
            raise

    def __h2o_connect(h2o_context):
        schema = h2o_context._jhc.h2oContext().getConf().getScheme()
        conf = h2o_context._conf

        kwargs = {}
        kwargs["https"] = schema == "https"
        kwargs["verify_ssl_certificates"] = conf.verifySslCertificates()
        if conf.userName() and conf.password():
            kwargs["auth"] = (conf.userName(), conf.password())
        url = "{}://{}:{}".format(schema, h2o_context._client_ip, h2o_context._client_port)
        if conf.contextPath() is not None:
            url = "{}/{}".format(url, conf.contextPath())
        return h2o.connect(url=url, **kwargs)


    @staticmethod
    def getOrCreate(spark=None, conf=None, **kwargs):
        """
        Get existing or create new H2OContext based on provided H2O configuration. If the conf parameter is set then
        configuration from it is used. Otherwise the configuration properties passed to Sparkling Water are used.
        If the values are not found the default values are used in most of the cases. The default cluster mode
        is internal, ie. spark.ext.h2o.external.cluster.mode=false

        :param spark: Spark Context or Spark Session or H2OConf
        :param conf: H2O configuration as instance of H2OConf
        :param kwargs:  additional parameters which are passed to h2o_connect_hook
        :return:  instance of H2OContext
        """

        if spark is not None and not isinstance(spark, H2OConf):
            warnings.warn("Method getOrCreate with spark argument is deprecated. Please use either just getOrCreate() or if you need "
                          "to pass extra H2OConf, use getOrCreate(conf). The spark argument will be removed in release 3.32.")

        # Workaround for bug in Spark 2.1 as SparkSession created in PySpark is not seen in Java
        # and call SparkSession.builder.getOrCreate on Java side creates a new session, which is not
        # desirable
        activeSession = SparkSession._instantiatedSession
        if activeSession is not None:
            jvm = activeSession.sparkContext._jvm
            jvm.org.apache.spark.sql.SparkSession.setDefaultSession(activeSession._jsparkSession)

        if spark is not None and isinstance(spark, H2OConf):
            selected_conf = spark
        elif conf is not None:
            selected_conf = conf
        else:
            selected_conf = H2OConf()
        if "auth" in kwargs:
            warnings.warn("Providing authentication via auth field on H2OContext is deprecated. "
                          "Please use setUserName and setPassword setters on H2OConf object.")
            selected_conf.setUserName(kwargs["auth"][0])
            selected_conf.setPassword(kwargs["auth"][1])
            del kwargs["auth"]
        if "verify_ssl_certificate" in kwargs:
            warnings.warn("Passing verify_ssl_certificate on H2OContext is deprecated. "
                          "Please use setVerifySslCertificates on H2OConf object.")
            selected_conf.setVerifySslCertificates(kwargs["verify_ssl_certificate"])
            del kwargs["verify_ssl_certificate"]
        h2o_context = H2OContext()

        # Create backing Java H2OContext
        jhc = _jvm().org.apache.spark.h2o.JavaH2OContext.getOrCreate(selected_conf._jconf)
        h2o_context._jhc = jhc
        h2o_context._conf = selected_conf
        h2o_context._client_ip = jhc.h2oLocalClientIp()
        h2o_context._client_port = jhc.h2oLocalClientPort()

        # Create H2O REST API client
        if not h2o_context.__isClientConnected() or not H2OContext.__isConnected:
            h2o_context.__h2o_connect()
            H2OContext.__isConnected = True
            h2o_context.__setClientConnected()
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
        warnings.warn("Method 'download_h2o_logs' is deprecated and will be removed in release 3.30. Please"
                      " use 'downloadH2OLogs' instead.")
        return self.downloadH2OLogs(destination, container)

    def downloadH2OLogs(self,  destination, container = "ZIP"):
        assert_is_type(container, Enum("ZIP", "LOG"))
        return self._jhc.h2oContext().downloadH2OLogs(destination, container)

    def __str__(self):
        if self.__isClientConnected() and not self.__isStopped():
            return self._jhc.toString()
        else:
            return "H2OContext has been stopped or hasn't been created. Call H2OContext.getOrCreate() or " \
                   "H2OContext.getOrCreate(conf) to create a new one."

    def __repr__(self):
        self.show()
        return ""

    def show(self):
        print(self)

    def get_conf(self):
        warnings.warn("Method 'get_conf' is deprecated and will be removed in release 3.30. Please"
                      " use 'getConf' instead.")
        return self.getConf()

    def getConf(self):
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
                    if sparkFrame.min() >= _jvm().Integer.MIN_VALUE and sparkFrame.max() <= _jvm().Integer.MAX_VALUE:
                        return fc._as_h2o_frame_from_RDD_Int(self, sparkFrame, h2oFrameName, fullCols)
                    elif sparkFrame.min() >= _jvm().Long.MIN_VALUE and sparkFrame.max() <= _jvm().Long.MAX_VALUE:
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
