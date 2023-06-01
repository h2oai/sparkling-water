import os
import subprocess
from ai.h2o.sparkling.Initializer import Initializer


def testSubmissionSparklingImportBeforeSparkCreated():
    cmd = [os.environ["PYSPARK_DRIVER_PYTHON"], "build/tests/unit/simple/pySparklingImportLoadsJustScoringJar.py"]
    returnCode = subprocess.call(cmd)
    assert returnCode == 0, "Process ended in a wrong way. It ended with return code " + str(returnCode)


def testSparkSubmitOptsNoJars():
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.app.name=test"
    jar = Initializer._Initializer__get_sw_jar(None)
    Initializer._Initializer__setUpPySparkSubmitArgs()
    propEdited = os.environ["PYSPARK_SUBMIT_ARGS"]
    del os.environ["PYSPARK_SUBMIT_ARGS"]
    assert propEdited == "--jars {} --conf spark.app.name=test".format(jar)


def testSparkSubmitOptsWithJars():
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.app.name=test --jars     dummy.jar"
    jar = Initializer._Initializer__get_sw_jar(None)
    Initializer._Initializer__setUpPySparkSubmitArgs()
    propEdited = os.environ["PYSPARK_SUBMIT_ARGS"]
    del os.environ["PYSPARK_SUBMIT_ARGS"]
    assert propEdited == "--conf spark.app.name=test --jars     {},dummy.jar".format(jar)
