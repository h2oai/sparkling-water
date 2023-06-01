import os
import sys
import pytest

dist = sys.argv[2]
path = os.getenv("PYTHONPATH")
if path is None:
    path = dist
else:
    path = "{}:{}".format(dist, path)

os.putenv("PYTHONPATH", path)
os.putenv('PYSPARK_DRIVER_PYTHON', sys.executable)
os.putenv('PYSPARK_PYTHON', sys.executable)

os.environ["PYTHONPATH"] = path
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_PYTHON'] = sys.executable

sys.path.insert(0, dist)
pytestConfigArgs = sys.argv[1].replace("'", "").split(" ")
args = pytestConfigArgs + ["-s", "--dist", dist, "--spark_conf", ' '.join(sys.argv[3:])]
code = pytest.main(args)
sys.exit(code)
