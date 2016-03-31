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
from pyspark.sql import SQLContext
import subprocess
from random import randrange

class IntegTestEnv:
    def __init__(self):
        self.spark_master = IntegTestEnv.get_env_org_fail("MASTER",
                                                          "The variable 'MASTER' should point to Spark cluster")
        self.hdp_version = IntegTestEnv.get_env_org_fail("sparkling.test.hdp.version",
                                                         "The variable 'sparkling.test.hdp.version' is not set! It should containing version of hdp used")
        self.egg = IntegTestEnv.get_env_org_fail("sparkling.pysparkling.egg",
                                                              "The variable 'sparkling.pysparkling.egg' is not set! It should point pySparkling egg file")
        self.spark_conf = {}
        self.verbose = True

    @staticmethod
    def get_env_org_fail(prop_name, fail_msg):
        try:
            return os.environ[prop_name]
        except KeyError:
            print fail_msg
            sys.exit(1)


class IntegTestSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_env = IntegTestEnv()

    @classmethod
    def tearDownClass(cls):
        cls.test_env = None

    @staticmethod
    def configure(app_name="Sparkling Water Demo"):
        conf = SparkConf()
        conf.setAppName(app_name)
        conf.setIfMissing("spark.master", os.getenv("spark.master", "local[*]"))
        return conf

    def launch(self, script_name):
        spark_home = os.environ["SPARK_HOME"]
        cmd_line = [IntegTestSuite.get_submit_script(spark_home)]
        cmd_line.append("--verbose")
        cmd_line.extend(["--master", self.test_env.spark_master])
        if self.test_env.spark_conf.has_key("spark.driver.memory"):
            cmd_line.extend(["--driver-memory", self.test_env.spark_conf.get("spark.driver.memory")])
        # Disable GA collection by default
        cmd_line.extend(["--conf", 'spark.ext.h2o.disable.ga=true'])
        # remove ".py" from cloud name
        cmd_line.extend(["--conf", 'spark.ext.h2o.cloud.name=sparkling-water-'+script_name[:-3]+randrange(65536)])
        cmd_line.extend(["--conf", '"spark.driver.extraJavaOptions=-XX:MaxPermSize=384m -Dhdp.version='+self.test_env.hdp_version+'"'])
        cmd_line.extend(["--conf", '"spark.yarn.am.extraJavaOptions=-Dhdp.version='+self.test_env.hdp_version+'"'])
        cmd_line.extend(["--conf", 'spark.test.home='+spark_home])
        cmd_line.extend(["--conf", 'spark.scheduler.minRegisteredResourcesRatio=1'])
        cmd_line.extend(["--conf", 'spark.ext.h2o.repl.enabled=false']) #  disable repl in tests
        cmd_line.extend(["--py-files", self.test_env.egg])
        for k, v in self.test_env.spark_conf.items():
            cmd_line.extend(["--conf", k+'='+str(v)])
        # Add python script
        cmd_line.append(script_name)
        # Launch it via command line
        return_code = subprocess.call(' '.join(cmd_line), shell=True)
        self.assertTrue(return_code == 0, "Process ended in a wrong way")

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
