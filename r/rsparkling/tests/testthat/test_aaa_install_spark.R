
context("Test if spark is installed, if it isn't it will install spark")

test_that("Test spark installation", {
  
  expected_version <- getOption("rsparkling.spark.version")
  
  installed_version <- spark_installed_versions()
  expected_installed <- expected_version %in% installed_version$spark
  
  if(!expected_installed) {
    inst <- spark_install(version=expected_version)
    expect_true(is.list(inst) && length(inst))
    installed_version <- spark_installed_versions()
    expected_installed <- expected_version %in% installed_version$spark
  }
  
  expect_true(expected_installed)
})
