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
import sys


def get_default_spark_conf(additional_conf=None):
    conf = {
        "spark.ext.h2o.external.start.mode": "auto",
        # Need to disable timeline service which requires Jersey libraries v1, but which are not available in Spark2.0
        # See: https://www.hackingnote.com/en/spark/trouble-shooting/NoClassDefFoundError-ClientConfig/
        "spark.hadoop.yarn.timeline-service.enabled": "false",
        "spark.scheduler.minRegisteredResourcesRatio": "1",
        "spark.ext.h2o.repl.enabled": "false",  # disable repl in tests
        "spark.ext.h2o.hadoop.memory": "2G",
        "spark.ext.h2o.port.base": "63331",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
        "spark.ext.h2o.node.log.dir": "build/h2ologs-pyIntegTest",
        "spark.ext.h2o.external.cluster.size": "1",
        "spark.ext.h2o.client.log.level": "WARN",
        "spark.ext.h2o.node.log.level": "WARN"
    }

    for key in additional_conf:
        conf[key] = additional_conf[key]

    return conf


def launch(conf, script_name):
    cmd_line = [
        get_submit_script(conf["spark.test.home"]),
        "--verbose",
        "--py-files", conf["spark.submit.pyFiles"]]

    for key, value in conf.items():
        cmd_line.extend(["--conf", key + '=' + str(value)])

    # Add path to test script
    cmd_line.append(script_name)

    # Launch it via command line
    return_code = subprocess.call(cmd_line)

    return return_code


# Determines whether we run on Windows or Unix and return correct spark-submit script location
def get_submit_script(spark_home):
    if sys.platform.startswith('win'):
        return spark_home + "\\bin\\spark-submit.cmd"
    else:
        return spark_home + "/bin/spark-submit"
