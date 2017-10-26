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
import subprocess
import generic_test_utils
from external_backend_manual_test_starter import ExternalBackendManualTestStarter


class IntegTestEnv:

    def __init__(self):

        self.spark_home = generic_test_utils.get_env_org_fail("SPARK_HOME", "The variable 'SPARK_HOME' should point to Spark home directory.")

        self.spark_master = generic_test_utils.get_env_org_fail("MASTER",
                                                          "The variable 'MASTER' should contain Spark cluster mode.")

        self.hdp_version = generic_test_utils.get_env_org_fail("sparkling.test.hdp.version",
                                                         "The variable 'sparkling.test.hdp.version' is not set! It should contain version of hdp used")

        self.sdist = generic_test_utils.get_env_org_fail("sparkling.pysparkling.sdist",
                                                              "The variable 'sparkling.pysparkling.sdist' is not set! It should point to PySparkling sdist file")
        self.spark_conf = {}
        self.verbose = True

    def conf(self, prop, val):
        self.spark_conf.update()
        self.spark_conf.update({prop: val})

    def set_spark_master(self, master):
        self.spark_master = master


def launch(test_env, script_name):
    cloud_name = generic_test_utils.unique_cloud_name(script_name)
    client_ip = generic_test_utils.local_ip()
    if generic_test_utils.tests_in_external_mode() and generic_test_utils.is_manual_cluster_start_mode_used():
        external_cluster_test_helper = ExternalBackendManualTestStarter()
        external_cluster_test_helper.start_cloud(2, cloud_name, client_ip)


    cmd_line = [get_submit_script(test_env.spark_home), "--verbose"]
    cmd_line.extend(["--master", test_env.spark_master])
    if test_env.spark_conf.has_key("spark.driver.memory"):
        cmd_line.extend(["--driver-memory", test_env.spark_conf.get("spark.driver.memory")])
    # Disable GA collection by default
    cmd_line.extend(["--conf", 'spark.ext.h2o.disable.ga=true'])
    # remove ".py" from cloud name
    cmd_line.extend(["--conf", 'spark.ext.h2o.cloud.name=sparkling-water-' + cloud_name])
    cmd_line.extend(["--conf", '"spark.driver.extraJavaOptions=-Dhdp.version=' + test_env.hdp_version+'"'])
    cmd_line.extend(["--conf", '"spark.yarn.am.extraJavaOptions=-Dhdp.version=' + test_env.hdp_version+'"'])
    cmd_line.extend(["--conf", 'spark.test.home=' + test_env.spark_home])
    cmd_line.extend(["--conf", 'spark.scheduler.minRegisteredResourcesRatio=1'])
    cmd_line.extend(["--conf", 'spark.ext.h2o.repl.enabled=false']) #  disable repl in tests
    cmd_line.extend(["--conf", "spark.ext.h2o.external.start.mode=" + os.getenv("spark.ext.h2o.external.start.mode", "manual")])
    # Need to disable timeline service which requires Jersey libraries v1, but which are not available in Spark2.0
    # See: https://www.hackingnote.com/en/spark/trouble-shooting/NoClassDefFoundError-ClientConfig/
    cmd_line.extend(["--conf", 'spark.hadoop.yarn.timeline-service.enabled=false'])
    cmd_line.extend(["--py-files", test_env.sdist])
    for k, v in test_env.spark_conf.items():
        cmd_line.extend(["--conf", k+'='+str(v)])

    if generic_test_utils.tests_in_external_mode():
        cloud_ip = generic_test_utils.local_ip()
        test_env.conf("spark.ext.h2o.client.ip", cloud_ip)
        test_env.conf("spark.ext.h2o.backend.cluster.mode", "external")
        test_env.conf("spark.ext.h2o.external.cluster.num.h2o.nodes", "2")
    else:
        test_env.conf("spark.ext.h2o.backend.cluster.mode", "internal")

    # Add python script
    cmd_line.append(script_name)
    # Launch it via command line
    return_code = subprocess.call(cmd_line)

    if generic_test_utils.tests_in_external_mode() and generic_test_utils.is_manual_cluster_start_mode_used():
        external_cluster_test_helper.stop_cloud()

    return return_code

# Determines whether we run on Windows or Unix and return correct spark-submit script location
def get_submit_script(spark_home):
    if sys.platform.startswith('win'):
        return spark_home+"\\bin\\spark-submit.cmd"
    else:
        return spark_home+"/bin/spark-submit"


