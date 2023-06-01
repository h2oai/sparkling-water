context("Test H2OConf setters are working correctly")

config <- spark_config()
config <- c(config, list(
  "spark.hadoop.yarn.timeline-service.enabled" = "false",
  "spark.ext.h2o.external.cluster.size" = "1",
  "spark.ext.h2o.backend.cluster.mode" = Sys.getenv("spark.ext.h2o.backend.cluster.mode"),
  "sparklyr.connect.enablehivesupport" = FALSE,
  "sparklyr.gateway.connect.timeout" = 240,
  "sparklyr.gateway.start.timeout" = 240,
  "sparklyr.backend.timeout" = 240,
  "sparklyr.log.console" = TRUE,
  "spark.ext.h2o.external.start.mode" = "auto",
  "spark.ext.h2o.external.disable.version.check" = "true",
  "sparklyr.gateway.port" = 55555,
  "sparklyr.connect.timeout" = 60 * 5,
  "spark.master" = "local[*]"
))

for (i in 1:4) {
  tryCatch(
    {
    sc <- spark_connect(master = "local[*]", config = config)
  }, error = function(e) { }
  )
}

test_that("test non overloaded setter without argument", {
  conf <- H2OConf()$useManualClusterStart()
  expect_equal(conf$isManualClusterStartUsed(), TRUE)
})

test_that("test non overloaded setter with argument", {
  conf <- H2OConf()$setExternalMemory("24G")
  expect_equal(conf$externalMemory(), "24G")
})

test_that("test non overloaded setter with wrong argument type", {
  expect_error(H2OConf()$setExternalMemory(24L))
})

test_that("test overloaded setter with two arguments", {
  conf <- H2OConf()$setH2OCluster("my_host", 8765L)
  expect_equal(conf$h2oClusterHost(), "my_host")
  expect_equal(conf$h2oClusterPort(), 8765)
})

test_that("test overloaded setter with one argument", {
  conf <- H2OConf()$setH2OCluster("my_host:6543")
  expect_equal(conf$h2oClusterHost(), "my_host")
  expect_equal(conf$h2oClusterPort(), 6543)
})

test_that("test overloaded setter with wrong argument type", {
 expect_error(H2OConf()$setH2OCluster(42))
})

test_that("test overloaded setter with string argument type", {
  conf <- H2OConf()$setExternalExtraJars("path1,path2,path3")
  expect_equal(conf$externalExtraJars(), "path1,path2,path3")
})

test_that("test overloaded setter with list argument type", {
  conf <- H2OConf()$setExternalExtraJars(list("path1", "path2", "path3"))
  expect_equal(conf$externalExtraJars(), "path1,path2,path3")
})
