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

"""
Helper class for running tests in external h2o cluster
"""

from random import randrange
import subprocess
import time
import test_utils
import socket

class ExternalClusterTestHelper:
    def __init__(self):
        self.sw_jar = test_utils.get_env_org_fail("sparkling.assembly.jar",
                                                                   "sparkling.assembly.jar environment variable is not set! It should " +
                                                                   "point to the location of sparkling-water assembly JAR")
        self.h2o_jar = test_utils.get_env_org_fail("H2O_JAR",
                                              "H2O_JAR environment variable is not set! It should point to the " +
                                              "location of H2O assembly jar file")
        self.node_processes = []


    @staticmethod
    def unique_cloud_name(custom_part):
        return "sparkling-water-"+custom_part+str(randrange(65536))

    @staticmethod
    def local_ip():
        return socket.gethostbyname(socket.gethostname())

    def _launch_single(self, cloud_name, ip):
        cmd_line = ["java", "-cp", ":".join([self.sw_jar, self.h2o_jar]), "water.H2OApp", "-md5skip", "-name", cloud_name, "-ip", ip]
        return subprocess.Popen(cmd_line)

    def start_cloud(self, cloud_size, cloud_name, ip):
        self.node_processes = [ self._launch_single(cloud_name, ip) for i in range(1, cloud_size+1)]
        # Wait 2 seconds to ensure that h2o nodes are created earlier than h2o client
        time.sleep(2)

    def stop_cloud(self):
        for p in self.node_processes:
            p.terminate()

    @staticmethod
    def tests_in_external_mode(spark_conf = None):
        if spark_conf is not None:
            return spark_conf.get("spark.ext.h2o.backend.cluster.mode", "internal") == "external"
        else:
            cluster_mode = test_utils.get_env_org_fail("spark.ext.h2o.backend.cluster.mode",
                                                        "The variable 'spark.ext.h2o.backend.cluster.mode' has to be set")
        return cluster_mode == "external"


