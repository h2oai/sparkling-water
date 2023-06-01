import time

from tests.integration.external_only.external_backend_test_utils import *
from tests.integration.integ_test_utils import *


def testStoppingWithoutExplicitStop(integ_spark_conf):
    return_code = launch(integ_spark_conf, "examples/tests/H2OContextWithoutExplicitStop.py")
    time.sleep(10)
    assert noYarnApps()
    assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)


def testStoppingWithExplicitStop(integ_spark_conf):
    return_code = launch(integ_spark_conf, "examples/tests/H2OContextWithExplicitStop.py")
    time.sleep(10)
    assert noYarnApps()
    assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)
