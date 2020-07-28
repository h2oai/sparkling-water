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

import pytest
from pysparkling.context import H2OContext

from tests.integration.external_only.external_backend_test_utils import *
from tests.unit_test_utils import *


def testH2OContextGetOrCreateReturnsReferenceToTheSameClusterIfStartedAutomatically(spark):
    context1 = H2OContext.getOrCreate(createH2OConf())
    context2 = H2OContext.getOrCreate(createH2OConf())

    getNodes = lambda context: context._jhc.getH2ONodes()
    toIpPort = lambda node: node.ipPort()
    nodesToString = lambda nodes: ', '.join(nodes)

    nodes1 = map(toIpPort, getNodes(context1))
    nodes2 = map(toIpPort, getNodes(context2))

    assert nodesToString(nodes1) == nodesToString(nodes2)
    context1.stop()


def testDownloadLogsAsLOG(spark):
    hc = H2OContext.getOrCreate(createH2OConf())
    path = hc.downloadH2OLogs("build", "LOG")
    clusterName = hc._conf.cloudName()

    with open(path, 'r') as f:
        lines = list(filter(lambda line: "INFO water.default: H2O cloud name: '" + clusterName + "'" in line, f.readlines()))
        assert len(lines) >= 1
    hc.stop()


def testDownloadLogsAsZIP(spark):
    hc = H2OContext.getOrCreate(createH2OConf())
    path = hc.downloadH2OLogs("build", "ZIP")
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

    context1 = H2OContext.getOrCreate(createH2OConf())
    yarnAppId1 = str(context1.getConf().get("spark.ext.h2o.external.yarn.app.id"))
    assert yarnAppId1 in listYarnApps()
    context1.stop()
    assert context1.__str__().startswith("H2OContext has been stopped or hasn't been created.")
    context2 = H2OContext.getOrCreate(createH2OConf())
    yarnAppId2 = str(context2.getConf().get("spark.ext.h2o.external.yarn.app.id"))
    assert yarnAppId1 not in listYarnApps()
    assert "Orderly shutdown:  Shutting down now." in yarnLogs(yarnAppId1)
    assert yarnAppId2 in listYarnApps()
    context2.stop()


def testConversionWorksAfterNewlyStartedContext(spark):
    context1 = H2OContext.getOrCreate(createH2OConf())
    context1.stop()
    context2 = H2OContext.getOrCreate(createH2OConf())
    rdd = spark.sparkContext.parallelize([0.5, 1.3333333333, 178])
    h2o_frame = context2.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == 0.5
    assert pytest.approx(h2o_frame[1, 0]) == 1.3333333333
    asert_h2o_frame(h2o_frame, rdd)
    context2.stop()
