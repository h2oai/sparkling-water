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

import unittest
import os
from pyspark import SparkContext, SparkConf
from external_cluster_test_utils import ExternalClusterTestHelper
import sys



def asert_h2o_frame(test_suite, h2o_frame, rdd):
    test_suite.assertEquals(h2o_frame.nrow, rdd.count(),"Number of rows should match")
    test_suite.assertEquals(h2o_frame.ncol, 1,"Number of columns should equal 1")
    test_suite.assertEquals(h2o_frame.names, ["values"],"Column should be name values")


def get_default_spark_conf():
    conf = SparkConf(). \
        setAppName("pyunit-test"). \
        setMaster("local-cluster[3,1,2048]"). \
        set("spark.ext.h2o.disable.ga","true"). \
        set("spark.driver.memory", "2g"). \
        set("spark.executor.memory", "2g"). \
        set("spark.ext.h2o.client.log.level", "DEBUG"). \
        set("spark.ext.h2o.repl.enabled", "false"). \
        set("spark.task.maxFailures", "1"). \
        set("spark.rpc.numRetries", "1"). \
        set("spark.deploy.maxExecutorRetries", "1"). \
        set("spark.ext.h2o.backend.cluster.mode", ExternalClusterTestHelper.cluster_mode()). \
        set("spark.ext.h2o.cloud.name", ExternalClusterTestHelper.unique_cloud_name("test")). \
        set("spark.ext.h2o.external.start.mode", os.getenv("spark.ext.h2o.external.start.mode", "manual")) .\
        set("spark.sql.warehouse.dir", "file:" + os.path.join(os.getcwd(), "spark-warehouse"))


    if ExternalClusterTestHelper.tests_in_external_mode():
        conf.set("spark.ext.h2o.client.ip", ExternalClusterTestHelper.local_ip())
        conf.set("spark.ext.h2o.external.cluster.num.h2o.nodes", "2")

    return conf

def set_up_class(cls):
    if ExternalClusterTestHelper.tests_in_external_mode(cls._sc._conf):
        cls.external_cluster_test_helper = ExternalClusterTestHelper()
        cloud_name = cls._sc._conf.get("spark.ext.h2o.cloud.name")
        cloud_ip = cls._sc._conf.get("spark.ext.h2o.client.ip")
        cls.external_cluster_test_helper.start_cloud(2, cloud_name, cloud_ip)


def tear_down_class(cls):
    if ExternalClusterTestHelper.tests_in_external_mode(cls._sc._conf):
        cls.external_cluster_test_helper.stop_cloud()
    cls._sc.stop()

# Runs python tests and by default reports to console.
# If filename is specified it additionally reports output to file with that name into py/build directory

def run_tests(cases, file_name=None):
    alltests = [unittest.TestLoader().loadTestsFromTestCase(case) for case in cases]
    testsuite = unittest.TestSuite(alltests)

    result = None
    if file_name is not None:
        reports_file = 'build'+os.sep+file_name+".txt"
        f = open(reports_file, "w")
        result = unittest.TextTestRunner(f, verbosity=2).run(testsuite)
        f.close()

        # Print output to console without running the tests again
        with open(reports_file, 'r') as fin:
            print(fin.read())
    else:
        # Run tests and print to console
        result = unittest.TextTestRunner(verbosity=2).run(testsuite)

    if result is not None:
        if len(result.errors) > 0 or len(result.failures) > 0:
            raise Exception(result)

def get_env_org_fail(prop_name, fail_msg):
    try:
        return os.environ[prop_name]
    except KeyError:
        print(fail_msg)
        sys.exit(1)