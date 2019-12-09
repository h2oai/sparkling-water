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

from pysparkling.context import H2OContext
from tests.unit.with_runtime_clientless_sparkling.clientless_test_utils import *


def testH2OContextGetOrCreateReturnsReferenceToTheSameClusterIfStartedAutomatically(spark):
    context1 = H2OContext.getOrCreate(spark, createH2OConf(spark))
    context2 = H2OContext.getOrCreate(spark, createH2OConf(spark))

    getNodes = lambda context: context._jhc.h2oContext().getH2ONodes()
    toIpPort = lambda node: node.ipPort()
    nodesToString = lambda nodes: ', '.join(nodes)

    nodes1 = map(toIpPort, getNodes(context1))
    nodes2 = map(toIpPort, getNodes(context2))

    assert nodesToString(nodes1) == nodesToString(nodes2)

def testDownloadLogsAsLOG(hc):
    path = hc.download_h2o_logs(".", "LOG")
    clusterName = hc.conf.cloud_name()

    with open(path, 'r') as f:
        lines = filter(lambda line: "INFO: H2O cloud name: '" + clusterName + "'" in line, f.readlines())
        assert len(lines) >= 1

def testDownloadLogsAsZIP(hc):
    path = hc.download_h2o_logs(".", "ZIP")
    import zipfile
    archive = zipfile.ZipFile(path, 'r')
    # The zip should have nested zip files for each node in the cluster + 1 for the parent directory
    assert len(archive.namelist()) == conf.cluster_size() + 1

