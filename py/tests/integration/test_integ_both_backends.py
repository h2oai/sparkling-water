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
import subprocess
import time
from py4j.java_gateway import *
from pyspark.context import SparkContext
import requests
import json

from tests.integration.integ_test_utils import *


def testPy4jGatewayConnection(integ_spark_conf):
    token = "my_super_secret_token"
    generateSSLFiles(token)
    startJavaGateway(integ_spark_conf, token)
    spark = obtainSparkSession(token)
    spark.sparkContext.parallelize([1, 2, 3, 4, 5]).collect()
    from pysparkling import H2OContext
    hc = H2OContext.getOrCreate()
    print(hc)
    hc.stop()


def testHamOrSpamPipelineAlgos(integ_spark_conf):
    return_code = launch(integ_spark_conf, "examples/HamOrSpamMultiAlgorithmDemo.py")
    assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)


def testChicagoCrime(integ_spark_conf):
    return_code = launch(integ_spark_conf, "examples/ChicagoCrimeDemo.py")
    assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)


def testImportPysparklingStandaloneApp(integ_spark_conf):
    return_code = launch(integ_spark_conf, "examples/tests/pysparkling_ml_import_overrides_spark_test.py")
    assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)


def generateSSLFiles(token):
    # CA key in PKCS1 format
    os.system('openssl genrsa -out build/ca.key 2048')
    # CA certificate
    os.system(
        'openssl req -x509 -new -nodes -key build/ca.key -sha256 -days 1825 -out build/ca.crt -subj "/CN=localhost"')
    # Server Key in PKCS8 format
    os.system('openssl genpkey -out build/server.key -algorithm RSA -pkeyopt rsa_keygen_bits:2048')
    # Server certificate signing request
    os.system('openssl req -new -key build/server.key -out build/server.csr -subj "/CN=localhost"')
    # Create config file required for signing
    with open('build/server.ext', 'w') as f:
        f.write('''
                authorityKeyIdentifier=keyid,issuer
                basicConstraints=CA:FALSE
                keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
                subjectAltName = @alt_names
                
                [alt_names]
                DNS.1 = localhost
                ''')
    # Server certificate
    os.system(
        'openssl x509 -req -in build/server.csr -CA build/ca.crt -CAkey build/ca.key -CAcreateserial -out build/server.crt -days 825 -sha256 -extfile build/server.ext')
    # Keystore containing the certificate and private key
    os.system(
        'openssl pkcs12 -export -inkey build/server.key -in build/server.crt -out build/keystore.pk12 -password pass:' + token)


def startJavaGateway(integ_spark_conf, token):
    with open('build/secret.txt', 'w') as f:
        f.write(token)

    cmd = [
        get_submit_script(integ_spark_conf["spark.test.home"]),
        "--class", "ai.h2o.sparkling.backend.SparklingGateway",
        "--conf", "spark.ext.h2o.py4j.gateway.secret.file.name=secret.txt",
        "--conf", "spark.ext.h2o.py4j.gateway.keystore.file.name=keystore.pk12",
        "--conf", "spark.master=local[*]",
        "--files", "build/secret.txt,build/keystore.pk12",
        integ_spark_conf["spark.ext.h2o.testing.path.to.sw.jar"]]
    import sys
    proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
    time.sleep(60)
    return proc


def obtainSparkSession(token):
    import ssl
    client_ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    client_ssl_context.verify_mode = ssl.CERT_REQUIRED
    client_ssl_context.check_hostname = True
    client_ssl_context.load_verify_locations(cafile='build/ca.crt')

    link = "http://localhost:54323/3/option?name=spark.ext.h2o.py4j.gateway.port"
    f = requests.get(link)
    port = int(json.loads(f.text)["value"])
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(
            address="localhost",
            port=port,
            ssl_context=client_ssl_context,
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
    return SparkSession.builder.getOrCreate()
