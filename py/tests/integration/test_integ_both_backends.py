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

import subprocess
import time
from py4j.java_gateway import *
from pyspark.context import SparkContext

from tests.integration.integ_test_utils import *


# def testHamOrSpamPipelineAlgos(integ_spark_conf):
#     return_code = launch(integ_spark_conf, "examples/HamOrSpamMultiAlgorithmDemo.py")
#     assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)
#
#
# def testChicagoCrime(integ_spark_conf):
#     return_code = launch(integ_spark_conf, "examples/ChicagoCrimeDemo.py")
#     assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)
#
#
# def testImportPysparklingStandaloneApp(integ_spark_conf):
#     return_code = launch(integ_spark_conf, "examples/tests/pysparkling_ml_import_overrides_spark_test.py")
#     assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)

def testPy4jGatewayConnection(integ_spark_conf):
    token = "my_super_secret_token"
    startJavaGateway(integ_spark_conf, token)
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(
            address="127.0.0.1",
            port=55555,
            auth_token=token,
            auto_convert=True))
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.ml.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.resource.*")
    java_import(gateway.jvm, "org.apache.spark.sql.*")
    java_import(gateway.jvm, "org.apache.spark.sql.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
    java_import(gateway.jvm, "scala.Tuple2")
    jsc = gateway.jvm.org.apache.spark.api.java.JavaSparkContext(
        gateway.jvm.org.apache.spark.SparkContext.getOrCreate())
    SparkContext(gateway=gateway, jsc=jsc)
    from pyspark.sql.session import SparkSession
    spark = SparkSession.builder.getOrCreate()
    from pysparkling import H2OContext
    hc = H2OContext.getOrCreate()
    spark.stop()


def startJavaGateway(integ_spark_conf, token):
    with open('build/secret.txt', 'w') as f:
        f.write(token)

    cmd = [
        get_submit_script(integ_spark_conf["spark.test.home"]),
        "--class", "ai.h2o.sparkling.backend.python.SparklingPy4jGateway",
        "--conf", "spark.ext.h2o.py4j.gateway.port=55555",
        "--conf", "spark.ext.h2o.py4j.gateway.secret.file.name=secret.txt",
        "--files", "build/secret.txt",
        integ_spark_conf["spark.ext.h2o.testing.path.to.sw.jar"]]
    import sys
    proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
    time.sleep(60)
    return proc
