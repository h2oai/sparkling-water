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
import numbers
import warnings
from ai.h2o.sparkling.H2OConf import H2OConf
from ai.h2o.sparkling.Initializer import Initializer
from ai.h2o.sparkling.H2ODataFrameConverters import H2ODataFrameConverters
from h2o.frame import H2OFrame
from h2o.utils.typechecks import assert_is_type, Enum
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, BooleanType, IntegerType, LongType, FloatType


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
        schema = h2o_context._jhc.getConf().getScheme()
        conf = h2o_context._conf

        kwargs = {}
        kwargs["https"] = schema == "https"
        kwargs["verify_ssl_certificates"] = conf.verifySslCertificates()
        kwargs["cacert"] = conf.sslCACert()
        if conf.userName() and conf.password():
            kwargs["auth"] = (conf.userName(), conf.password())
        url = "{}://{}:{}".format(schema, h2o_context._client_ip, h2o_context._client_port)
        if conf.contextPath() is not None:
            url = "{}/{}".format(url, conf.contextPath())
        return h2o.connect(url=url, **kwargs)

    @staticmethod
    def getOrCreate(conf=None):
        """
        Get existing or create new H2OContext based on provided H2O configuration. If the conf parameter is set then
        configuration from it is used. Otherwise the configuration properties passed to Sparkling Water are used.
        If the values are not found the default values are used in most of the cases. The default cluster mode
        is internal, ie. spark.ext.h2o.external.cluster.mode=false

        :param conf: H2O configuration as instance of H2OConf
        :return:  instance of H2OContext
        """

        # Workaround for bug in Spark 2.1 as SparkSession created in PySpark is not seen in Java
        # and call SparkSession.builder.getOrCreate on Java side creates a new session, which is not
        # desirable
        activeSession = SparkSession._instantiatedSession
        jvm = activeSession.sparkContext._jvm
        jvm.org.apache.spark.sql.SparkSession.setDefaultSession(activeSession._jsparkSession)

        if conf is not None:
            selected_conf = conf
        else:
            selected_conf = H2OConf()
        selected_conf.set("spark.ext.h2o.client.language", "python")
        h2o_context = H2OContext()

        # Create backing H2OContext
        h2o_context._jvm = jvm
        package = getattr(jvm.ai.h2o.sparkling, "H2OContext$")
        module = package.__getattr__("MODULE$")
        jhc = module.getOrCreate(selected_conf._jconf)
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
        hc = self._jhc
        field = hc.getClass().getDeclaredField("stopped")
        field.setAccessible(True)
        return field.get(hc)

    def __isClientConnected(self):
        field = self.__getClientConnectedField()
        return field.get(self._jhc)

    def __setClientConnected(self):
        field = self.__getClientConnectedField()
        field.set(self._jhc, True)

    def __getClientConnectedField(self):
        field = self._jhc.getClass().getDeclaredField("clientConnected")
        field.setAccessible(True)
        return field

    def stop(self, stopSparkContext=False):
        h2o.connection().close()
        scalaStopMethod = getattr(self._jhc, "ai$h2o$sparkling$H2OContext$$stop")
        scalaStopMethod(stopSparkContext, False, False)  # stopSpark = False, stopJVM = False, inShutdownHook = False

    def downloadH2OLogs(self, destination, container="ZIP"):
        assert_is_type(container, Enum("ZIP", "LOG"))
        return self._jhc.downloadH2OLogs(destination, container)

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

    def getConf(self):
        return self._conf

    def setH2OLogLevel(self, level):
        self._jhc.setH2OLogLevel(level)

    def getH2OLogLevel(self):
        return self._jhc.getH2OLogLevel()

    def importHiveTable(self, database="default", table=None, partitions=None, allowMultiFormat=False):
        return h2o.import_hive_table(database, table, partitions, allowMultiFormat)

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
            jdf = self._jhc.asSparkFrame(frame_id, copyMetadata)
            df = H2ODataFrameConverters.scalaToPythonDataFrame(jdf)
            # Attach h2o_frame to dataframe which forces python not to delete the frame when we leave the scope of this
            # method.
            # Without this, after leaving this method python would garbage collect the frame since it's not used
            # anywhere and spark. when executing any action on this dataframe, will fail since the frame
            # would be missing.
            df._h2o_frame = h2oFrame
            return df

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
        assert_is_type(sparkFrame, DataFrame, RDD)

        df = H2OContext.__prepareSparkDataForConversion(self._jvm, sparkFrame)
        if h2oFrameName is None:
            key = self._jhc.asH2OFrame(df._jdf).frameId()
        else:
            key = self._jhc.asH2OFrame(df._jdf, h2oFrameName).frameId()
        return H2OFrame.get_frame(key, full_cols=fullCols, light=True)

    @staticmethod
    def __prepareSparkDataForConversion(_jvm, sparkData):
        if isinstance(sparkData, DataFrame):
            return sparkData
        elif sparkData.isEmpty():
            return sparkData.toDF()
        else:
            session = SparkSession.builder.getOrCreate()
            first = sparkData.first()
            if isinstance(first, (str, bool, numbers.Integral, float)):
                if isinstance(first, str):
                    return session.createDataFrame(sparkData, StringType())
                elif isinstance(first, bool):
                    return session.createDataFrame(sparkData, BooleanType())
                elif (isinstance(sparkData.min(), numbers.Integral) and isinstance(sparkData.max(), numbers.Integral)):
                    if sparkData.min() >= _jvm.Integer.MIN_VALUE and sparkData.max() <= _jvm.Integer.MAX_VALUE:
                        return session.createDataFrame(sparkData, IntegerType())
                    elif sparkData.min() >= _jvm.Long.MIN_VALUE and sparkData.max() <= _jvm.Long.MAX_VALUE:
                        return session.createDataFrame(sparkData, LongType())
                    else:
                        warnings.warn(
                            "Maximal or minimal number in RDD is too big to convert to Java. Treating numbers as strings.")
                        return session.createDataFrame(sparkData, StringType())
                elif isinstance(first, float):
                    ## Spark would fail when creating data frame if there is int type in RDD[Float]
                    ## Convert explicitly all to float
                    return session.createDataFrame(sparkData.map(lambda x: float(x)), FloatType())
                else:
                    raise ValueError('Unreachable code')
            else:
                return session.createDataFrame(sparkData)
