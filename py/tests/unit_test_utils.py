from pyspark import SparkConf
from generic_test_utils import *
import os

def asert_h2o_frame(test_suite, h2o_frame, rdd):
    test_suite.assertEquals(h2o_frame.nrow, rdd.count(),"Number of rows should match")
    test_suite.assertEquals(h2o_frame.ncol, 1,"Number of columns should equal 1")
    test_suite.assertEquals(h2o_frame.names, ["values"],"Column should be name values")


def get_default_spark_conf():
    conf = SparkConf(). \
        setAppName("pyunit-test"). \
        setMaster("local-cluster[3,1,2048]"). \
        set("spark.driver.memory", "2g"). \
        set("spark.executor.memory", "2g"). \
        set("spark.ext.h2o.client.log.level", "DEBUG"). \
        set("spark.ext.h2o.repl.enabled", "false"). \
        set("spark.task.maxFailures", "1"). \
        set("spark.rpc.numRetries", "1"). \
        set("spark.deploy.maxExecutorRetries", "1"). \
        set("spark.network.timeout", "360s"). \
        set("spark.worker.timeout", "360"). \
        set("spark.ext.h2o.backend.cluster.mode", cluster_mode()). \
        set("spark.ext.h2o.cloud.name", unique_cloud_name("test")). \
        set("spark.ext.h2o.external.start.mode", os.getenv("spark.ext.h2o.external.start.mode", "manual")). \
        set("spark.driver.extraClassPath", os.getenv("sparkling.mojo.pipeline.jar")). \
        set("spark.executor.extraClassPath", os.getenv("sparkling.mojo.pipeline.jar"))

    if tests_in_external_mode():
        conf.set("spark.ext.h2o.client.ip", local_ip())
        conf.set("spark.ext.h2o.external.cluster.num.h2o.nodes", "2")

    return conf

def set_up_class(cls):
    if tests_in_external_mode(cls._spark.conf) and is_manual_cluster_start_mode_used():
        cls.external_cluster_test_helper = ExternalBackendManualTestStarter()
        cloud_name = cls._spark.conf.get("spark.ext.h2o.cloud.name")
        cloud_ip = cls._spark.conf.get("spark.ext.h2o.client.ip")
        cls.external_cluster_test_helper.start_cloud(2, cloud_name, cloud_ip)


def tear_down_class(cls):
    if tests_in_external_mode(cls._spark.conf) and is_manual_cluster_start_mode_used():
        cls.external_cluster_test_helper.stop_cloud()
    cls._spark.stop()
