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
import unittest
from pyspark import SparkContext, SparkConf
import subprocess
from random import randrange
import test_utils
from external_cluster_test_utils import ExternalClusterTestHelper


class IntegTestEnv:
    def __init__(self):

        self.spark_home = test_utils.get_env_org_fail("SPARK_HOME","The variable 'SPARK_HOME' should point to Spark home directory.")

        self.spark_master = test_utils.get_env_org_fail("MASTER",
                                                          "The variable 'MASTER' should contain Spark cluster mode.")

        self.hdp_version = test_utils.get_env_org_fail("sparkling.test.hdp.version",
                                                         "The variable 'sparkling.test.hdp.version' is not set! It should contain version of hdp used")

        self.egg = test_utils.get_env_org_fail("sparkling.pysparkling.egg",
                                                              "The variable 'sparkling.pysparkling.egg' is not set! It should point to PySparkling egg file")
        self.spark_conf = {}
        self.verbose = True


class IntegTestSuite(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_env = IntegTestEnv()
        if ExternalClusterTestHelper.tests_in_external_mode():
            cloud_name = ExternalClusterTestHelper.unique_cloud_name("integ-test")
            cloud_ip = ExternalClusterTestHelper.local_ip()
            cls.external_cluster_test_helper = ExternalClusterTestHelper()
            cls.conf("spark.ext.h2o.cloud.name", cloud_name)
            cls.conf("spark.ext.h2o.client.ip", cloud_ip)
            cls.conf("spark.ext.h2o.backend.cluster.mode", "external")
            cls.external_cluster_test_helper.start_cloud(2, cloud_name, cloud_ip)
        else:
            cls.conf("spark.ext.h2o.backend.cluster.mode", "internal")



    @classmethod
    def tearDownClass(cls):
        if ExternalClusterTestHelper.tests_in_external_mode():
            cls.external_cluster_test_helper.stop_cloud()
        cls.test_env = None

    @staticmethod
    def configure(app_name="Sparkling Water Demo"):
        conf = SparkConf()
        conf.setAppName(app_name)
        conf.setIfMissing("spark.master", os.getenv("spark.master", "local[*]"))
        return conf

    def launch(self, script_name):
        cmd_line = [IntegTestSuite.get_submit_script(self.test_env.spark_home), "--verbose"]
        cmd_line.extend(["--master", self.test_env.spark_master])
        if self.test_env.spark_conf.has_key("spark.driver.memory"):
            cmd_line.extend(["--driver-memory", self.test_env.spark_conf.get("spark.driver.memory")])
        # Disable GA collection by default
        cmd_line.extend(["--conf", 'spark.ext.h2o.disable.ga=true'])
        # remove ".py" from cloud name
        cmd_line.extend(["--conf", 'spark.ext.h2o.cloud.name=sparkling-water-'+str(script_name[:-3])+str(randrange(65536))])
        cmd_line.extend(["--conf", '"spark.driver.extraJavaOptions=-XX:MaxPermSize=384m -Dhdp.version='+self.test_env.hdp_version+'"'])
        cmd_line.extend(["--conf", '"spark.yarn.am.extraJavaOptions=-Dhdp.version='+self.test_env.hdp_version+'"'])
        cmd_line.extend(["--conf", 'spark.test.home='+self.test_env.spark_home])
        cmd_line.extend(["--conf", 'spark.scheduler.minRegisteredResourcesRatio=1'])
        cmd_line.extend(["--conf", 'spark.ext.h2o.repl.enabled=false']) #  disable repl in tests
        cmd_line.extend(["--py-files", self.test_env.egg])
        for k, v in self.test_env.spark_conf.items():
            cmd_line.extend(["--conf", k+'='+str(v)])
        # Add python script
        cmd_line.append(script_name)
        # Launch it via command line
        return_code = subprocess.call(cmd_line)
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code "+str(return_code))

    # Determines whether we run on Windows or Unix and return correct spark-submit script location
    @staticmethod
    def get_submit_script(spark_home):
        if sys.platform.startswith('win'):
            return spark_home+"\\bin\\spark-submit.cmd"
        else:
            return spark_home+"/bin/spark-submit"

    @classmethod
    def conf(cls, prop, val):
        cls.test_env.spark_conf.update()
        cls.test_env.spark_conf.update({prop: val})

    def spark_master(self, master):
        self.test_env.spark_master = master



