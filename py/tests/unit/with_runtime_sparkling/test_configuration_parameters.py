from pysparkling.conf import H2OConf
import pytest


def test_non_overloaded_setter_without_arguments():
    conf = H2OConf().useManualClusterStart()
    assert (conf.isManualClusterStartUsed() is True)


def test_non_overloaded_setter_with_argument():
    conf = H2OConf().setExternalMemory("24G")
    assert (conf.externalMemory() == "24G")


def test_non_overloaded_setter_with_wrong_argument_type():
    with pytest.raises(Exception):
        H2OConf().setExternalMemory(24)


def test_overloaded_setter_with_two_arguments():
    conf = H2OConf().setH2OCluster("my_host", 8765)
    assert (conf.h2oClusterHost() == "my_host")
    assert (conf.h2oClusterPort() == 8765)


def test_overloaded_setter_with_one_argument():
    conf = H2OConf().setH2OCluster("my_host:6543")
    assert (conf.h2oClusterHost() == "my_host")
    assert (conf.h2oClusterPort() == 6543)


def test_overloaded_setter_with_wrong_argument_type():
    with pytest.raises(Exception):
        conf = H2OConf().setH2OCluster(42)


def test_overloaded_setter_with_string_argument():
    conf = H2OConf().setExternalExtraJars("path1,path2,path3")
    assert (conf.externalExtraJars() == "path1,path2,path3")


def test_overloaded_setter_with_list_argument():
    conf = H2OConf().setExternalExtraJars(["path1", "path2", "path3"])
    assert (conf.externalExtraJars() == "path1,path2,path3")

