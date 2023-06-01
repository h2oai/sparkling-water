import pytest


def pytest_addoption(parser):
    parser.addoption("--dist", action="store", default="")
    parser.addoption("--spark_conf", action="store", default="")


@pytest.fixture(scope="session")
def dist(request):
    return request.config.getoption("--dist")


@pytest.fixture(scope="session")
def spark_conf(request):
    conf = request.config.getoption("--spark_conf").split()
    m = {}
    for arg in conf:
        split = arg.split("=", 1)
        m[split[0]] = split[1]
    return m
