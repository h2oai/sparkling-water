# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import subprocess
from pysparkling.context import H2OContext

from tests.unit.with_runtime_clientless_sparkling.clientless_test_utils import *


def testStartWithSSLAndAuthorization(spark):
    with open('build/login.conf', 'w') as f:
        f.write('user:pass')

    conf = createH2OConf(spark)
    # Require authentication
    conf.set_hash_login_enabled()
    conf.set_login_conf("build/login.conf")
    # Secure External H2O backend by SSL
    conf.set_internal_secure_connections_enabled()

    context = H2OContext.getOrCreate(spark, conf, auth=("user", "pass"))
    yarnAppId = str(context._jhc.h2oContext().backend().yarnAppId().get())
    context.stop()
    output = str(subprocess.check_output("yarn logs -applicationId " + yarnAppId, shell=True))
    assert "H2O node running in encrypted mode using" in output
    assert "-hash_login" in output
