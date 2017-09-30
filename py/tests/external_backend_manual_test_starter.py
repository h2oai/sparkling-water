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
Helper class used to start H2O nodes from scala code
"""

import subprocess
import time
import os
import generic_test_utils


class ExternalBackendManualTestStarter:
    def __init__(self):
        self.sw_jar = generic_test_utils.get_env_org_fail("sparkling.assembly.jar",
                                                  "sparkling.assembly.jar environment variable is not set! It should " +
                                                  "point to the location of sparkling-water assembly JAR")
        self.h2o_extended_jar = generic_test_utils.get_env_org_fail("H2O_EXTENDED_JAR",
                                                            "H2O_EXTENDED_JAR environment variable is not set! It should point to the " +
                                                            "location of H2O assembly jar file")

        # input here is milliseconds to be consistent with scala tests, but python expects seconds
        self.cluster_start_timeout = int(os.getenv("cluster.start.timeout", "6000")) / 1000

        self.node_processes = []

    def launch_single(self, cloud_name, ip):
        cmd_line = ["java", "-cp", ":".join([self.h2o_extended_jar, self.sw_jar]), "water.H2OApp", "-name",
                    cloud_name, "-ip", ip]
        return subprocess.Popen(cmd_line)

    def start_cloud(self, cloud_size, cloud_name, ip):
        self.node_processes = [self.launch_single(cloud_name, ip) for i in range(1, cloud_size + 1)]
        # Wait to ensure that h2o nodes are created earlier than h2o client
        time.sleep(self.cluster_start_timeout)

    def stop_cloud(self):
        for p in self.node_processes:
            p.terminate()
