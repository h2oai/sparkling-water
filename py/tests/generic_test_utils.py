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
from pyspark import SparkConf
import sys
from random import randrange
import socket

def unique_cloud_name(script_name):
    return str(script_name[:-3])+str(randrange(65536))

def locate(file_name):
    if os.path.isfile("/home/0xdiag/" + file_name):
        return os.path.abspath("/home/0xdiag/" + file_name)
    else:
        return os.path.abspath("../examples/" + file_name)

def cluster_mode(spark_conf = None):
    if spark_conf is not None:
        return spark_conf.get("spark.ext.h2o.backend.cluster.mode", "internal")
    else:
        return os.getenv('spark.ext.h2o.backend.cluster.mode', "internal")

def tests_in_external_mode(spark_conf = None):
    return cluster_mode(spark_conf) == "external"

def tests_in_internal_mode(spark_conf = None):
    return not tests_in_external_mode(spark_conf)

def is_auto_cluster_start_mode_used():
    return os.getenv("spark.ext.h2o.external.start.mode", "manual") == "auto"

def is_manual_cluster_start_mode_used():
    return os.getenv("spark.ext.h2o.external.start.mode", "manual") == "manual"

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

def local_ip():
    return os.getenv("H2O_CLIENT_IP", get_local_non_loopback_ipv4_address())

def get_local_non_loopback_ipv4_address():
    ips1 = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1]
    ips2 = [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]
    return [l for l in (ips1, ips2) if l][0][0]
