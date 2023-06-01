
def testH2OConf(hc, spark):
    assert hc.getConf().logDir() == spark.conf.get("spark.ext.h2o.log.dir")
