# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pytest
from pysparkling.context import H2OContext

from tests.integration.external_only.external_backend_test_utils import *


def testSSL(spark):
    conf = createH2OConf()
    conf.setInternalSecureConnectionsEnabled()

    context = H2OContext.getOrCreate(conf)
    path = context.downloadH2OLogs("build", "LOG")

    with open(path, 'r') as f:
        originalLines = f.readlines()
        lines = list(filter(lambda line: "H2O node running in encrypted mode using" in line, originalLines))
        assert len(lines) >= 1
    context.stop()


def testAuth(spark):
    with open('build/login.conf', 'w') as f:
        f.write('user:pass')

    conf = createH2OConf()
    conf.setHashLoginEnabled()
    conf.setLoginConf("build/login.conf")
    conf.setUserName("user")
    conf.setPassword("pass")
    context = H2OContext.getOrCreate(conf)
    path = context.downloadH2OLogs("build", "LOG")

    with open(path, 'r') as f:
        originalLines = f.readlines()
        lines = list(filter(lambda line: "-hash_login" in line, originalLines))
        assert len(lines) >= 1
    context.stop()


def testAuthFailsWhenUsernamePasswordNotSpecified(spark):
    with open('build/login.conf', 'w') as f:
        f.write('user:pass')

    conf = createH2OConf()
    conf.setHashLoginEnabled()
    conf.setCloudName("test-cluster")
    conf.setClusterInfoFile("build/notify_file.txt")
    conf.setLoginConf("build/login.conf")

    with pytest.raises(Exception):
        H2OContext.getOrCreate(conf)
    # No app should be running
    assert noYarnApps()
    conf.setUserName("user")
    conf.setPassword("pass")
    context = H2OContext.getOrCreate(conf)
    context.stop()


def createPamLoginFile():
    with open('build/login.conf', 'w') as f:
        f.write('pamloginmodule {\n')
        f.write('     de.codedo.jaas.PamLoginModule required\n')
        f.write('     service = common-auth;\n')
        f.write('};\n')


def testPamAuthWithCorrectCredentials(spark):
    createPamLoginFile()
    conf = createH2OConf()
    conf.setPamLoginEnabled()
    conf.setCloudName("test-cluster")
    conf.setClusterInfoFile("build/notify_file.txt")
    conf.setLoginConf("build/login.conf")
    conf.setUserName("jenkins")
    conf.setPassword("jenkins")
    context = H2OContext.getOrCreate(conf)
    context.stop()


def testPamAuthWithWrongCredentials(spark):
    createPamLoginFile()
    conf = createH2OConf()
    conf.setPamLoginEnabled()
    conf.setCloudName("test-cluster")
    conf.setClusterInfoFile("build/notify_file.txt")
    conf.setLoginConf("build/login.conf")
    conf.setUserName("jenkins")
    conf.setPassword("wrong_password")

    with pytest.raises(Exception):
        H2OContext.getOrCreate(conf)
