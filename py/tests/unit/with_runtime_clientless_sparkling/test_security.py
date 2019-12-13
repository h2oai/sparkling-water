# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pytest
from pysparkling.context import H2OContext

from tests.unit.with_runtime_clientless_sparkling.clientless_test_utils import *


def testSSL(spark):
    conf = createH2OConf(spark)
    conf.set_internal_secure_connections_enabled()

    context = H2OContext.getOrCreate(spark, conf)
    path = context.download_h2o_logs("build", "LOG")

    with open(path, 'r') as f:
        originalLines = f.readlines()
        lines = list(filter(lambda line: "H2O node running in encrypted mode using" in line, originalLines))
        assert len(lines) >= 1
    context.stop()


def testAuth(spark):
    with open('build/login.conf', 'w') as f:
        f.write('user:pass')

    conf = createH2OConf(spark)
    conf.set_hash_login_enabled()
    conf.set_login_conf("build/login.conf")
    conf.setUserName("user")
    conf.setPassword("pass")
    context = H2OContext.getOrCreate(spark, conf)
    path = context.download_h2o_logs("build", "LOG")

    with open(path, 'r') as f:
        originalLines = f.readlines()
        lines = list(filter(lambda line: "-hash_login" in line, originalLines))
        assert len(lines) >= 1
    context.stop()

def testAuthFailsWhenUsernamePasswordNotSpecified(spark):
    with open('build/login.conf', 'w') as f:
        f.write('user:pass')

    conf = createH2OConf(spark)
    conf.set_hash_login_enabled()
    conf.set_cloud_name("test-cluster")
    conf.set_cluster_config_file("notify_file.txt")
    conf.set_login_conf("build/login.conf")

    with pytest.raises(Exception):
        H2OContext.getOrCreate(spark, conf)
    # No app should be running
    assert "Total number of applications (application-types: [] and states: [SUBMITTED, ACCEPTED, RUNNING]):0" in listYarnApps()
    conf.setUserName("user")
    conf.setPassword("pass")
    context = H2OContext.getOrCreate(spark, conf)
    context.stop()
