from tests.integration.integ_test_utils import *

def testHamOrSpamPipelineAlgos(integ_spark_conf):
    return_code = launch(integ_spark_conf, "examples/HamOrSpamMultiAlgorithmDemo.py")
    assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)


def testChicagoCrime(integ_spark_conf):
    return_code = launch(integ_spark_conf, "examples/ChicagoCrimeDemo.py")
    assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)
