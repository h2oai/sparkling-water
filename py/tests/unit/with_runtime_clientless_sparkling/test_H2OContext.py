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
import time

import requests
from pysparkling.context import H2OContext

from tests.unit_test_utils import *
from tests.unit.with_runtime_clientless_sparkling.clientless_test_utils import *


def testZombieExternalH2OCluster():
    jarPath = os.environ['H2O_DRIVER_JAR']
    notifyFile = "notify.txt"
    subprocess.check_call("hadoop jar {} -disown -notify {} -nodes 1 -mapperXmx 2G -J -rest_api_ping_timeout -J {}".format(jarPath, notifyFile, 10000), shell=True)
    ipPort = getIpPortFromNotifyFile(notifyFile)
    appId = getYarnAppIdFromNotifyFile(notifyFile)
    # Lock the cloud, from this time, the cluster should stop after 10 seconds if nothing pings the /3/Ping endpoint
    requests.post(url = "http://" + ipPort + "/3/CloudLock")

    # Keep pinging the cluster
    for x in range(0, 5):
        requests.get(url = "http://" + ipPort + "/3/Ping")
        assert appId in listYarnApps()
        time.sleep(5)
    # Wait 60 seconds, H2O cluster should shut down as nothing has touched the /3/Ping endpoint
    time.sleep(60)
    assert appId not in listYarnApps()
    logs = yarnLogs(appId).replace("haven\\'t", "haven't")
    assert "Stopping H2O cluster since we haven't received any REST api request on 3/Ping!" in logs


def testH2OContextGetOrCreateReturnsReferenceToTheSameClusterIfStartedAutomatically(spark):
    context1 = H2OContext.getOrCreate(spark, createH2OConf(spark))
    context2 = H2OContext.getOrCreate(spark, createH2OConf(spark))

    getNodes = lambda context: context._jhc.h2oContext().getH2ONodes()
    toIpPort = lambda node: node.ipPort()
    nodesToString = lambda nodes: ', '.join(nodes)

    nodes1 = map(toIpPort, getNodes(context1))
    nodes2 = map(toIpPort, getNodes(context2))

    assert nodesToString(nodes1) == nodesToString(nodes2)
    context1.stop()


def testDownloadLogsAsLOG(spark):
    hc = H2OContext.getOrCreate(spark, createH2OConf(spark))
    path = hc.download_h2o_logs("build", "LOG")
    clusterName = hc._conf.cloud_name()

    with open(path, 'r') as f:
        lines = list(filter(lambda line: "INFO: H2O cloud name: '" + clusterName + "'" in line, f.readlines()))
        assert len(lines) >= 1
    hc.stop()


def testDownloadLogsAsZIP(spark):
    hc = H2OContext.getOrCreate(spark, createH2OConf(spark))
    path = hc.download_h2o_logs("build", "ZIP")
    import zipfile
    archive = zipfile.ZipFile(path, 'r')
    # The zip should have nested zip files for each node in the cluster + 1 for the parent directory
    assert len(archive.namelist()) == 2
    hc.stop()


def testStopAndStartAgain(spark):
    import subprocess
    def listYarnApps():
        return str(subprocess.check_output("yarn application -list", shell=True))

    def yarnLogs(appId):
        return str(subprocess.check_output("yarn logs -applicationId " + appId, shell=True))

    context1 = H2OContext.getOrCreate(spark, createH2OConf(spark))
    yarnAppId1 = str(context1._jhc.h2oContext().backend().yarnAppId().get())
    assert yarnAppId1 in listYarnApps()
    context1.stop()
    assert context1.__str__().startswith("H2OContext has been stopped or hasn't been created.")
    context2 = H2OContext.getOrCreate(spark, createH2OConf(spark))
    yarnAppId2 = str(context2._jhc.h2oContext().backend().yarnAppId().get())
    assert yarnAppId1 not in listYarnApps()
    assert "Orderly shutdown:  Shutting down now." in yarnLogs(yarnAppId1)
    assert yarnAppId2 in listYarnApps()
    context2.stop()


def testConversionWorksAfterNewlyStartedContext(spark):
    context1 = H2OContext.getOrCreate(spark, createH2OConf(spark))
    context1.stop()
    context2 = H2OContext.getOrCreate(spark, createH2OConf(spark))
    rdd = spark.sparkContext.parallelize([0.5, 1.3333333333, 178])
    h2o_frame = context2.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == 0.5
    assert h2o_frame[1, 0] == 1.3333333333
    asert_h2o_frame(h2o_frame, rdd)
    context2.stop()
