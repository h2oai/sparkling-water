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

import subprocess
import time
from random import randrange
import os

import test_utils
import socket


class ExternalClusterTestHelper:
    def __init__(self):
        self.sw_jar = test_utils.get_env_org_fail("sparkling.assembly.jar",
                                                                   "sparkling.assembly.jar environment variable is not set! It should " +
                                                                   "point to the location of sparkling-water assembly JAR")
        self.h2o_extended_jar = test_utils.get_env_org_fail("H2O_EXTENDED_JAR",
                                              "H2O_EXTENDED_JAR environment variable is not set! It should point to the " +
                                              "location of H2O assembly jar file")

        # input here is milliseconds to be consistent with scala tests, but python expects seconds
        self.cluster_start_timeout = int(os.getenv("cluster.start.timeout", "6000"))/1000

        self.node_processes = []

    @staticmethod
    def tests_in_external_mode(spark_conf = None):
        if spark_conf is not None:
            return spark_conf.get("spark.ext.h2o.backend.cluster.mode", "internal") == "external"
        else:
            cluster_mode = ExternalClusterTestHelper.cluster_mode()
            return cluster_mode == "external"

    @staticmethod
    def tests_in_internal_mode(spark_conf = None):
        return not ExternalClusterTestHelper.tests_in_external_mode(spark_conf)

    @staticmethod
    def cluster_mode():
        return os.getenv('spark.ext.h2o.backend.cluster.mode', "internal")

    @staticmethod
    def unique_cloud_name(custom_part):
        return "sparkling-water-" + custom_part + str(randrange(65536))

    @staticmethod
    def local_ip():
        return os.getenv("H2O_CLIENT_IP", ExternalClusterTestHelper.get_local_non_loopback_ipv4_address())

    def _launch_single(self, cloud_name, ip):
        cmd_line = ["java", "-cp", ":".join([self.h2o_extended_jar, self.sw_jar]), "water.H2OApp", "-md5skip", "-name", cloud_name, "-ip", ip]
        return subprocess.Popen(cmd_line)

    @staticmethod
    def run_H2O_cluster_on_yarn():
        return os.getenv("spark.ext.h2o.external.start.mode", "manual") == "auto"

    def start_cloud(self, cloud_size, cloud_name, ip):
        #do not start h2o nodes if this property is set, they will be started on yarn automatically
        if not ExternalClusterTestHelper.run_H2O_cluster_on_yarn():
            self.node_processes = [self._launch_single(cloud_name, ip) for i in range(1, cloud_size + 1)]
            # Wait to ensure that h2o nodes are created earlier than h2o client
            time.sleep(self.cluster_start_timeout)

    def stop_cloud(self):
        if not ExternalClusterTestHelper.run_H2O_cluster_on_yarn():
                for p in self.node_processes:
                    p.terminate()

    @staticmethod
    def get_local_non_loopback_ipv4_address():
        ips1 = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1]
        ips2 = [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]
        return [l for l in (ips1, ips2) if l][0][0]
