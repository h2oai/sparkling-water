library(testthat)
library(dplyr)
library(sparklyr)
library(h2o)
library(rsparkling)

options(rsparkling.sparklingwater.version = Sys.getenv("SPARKLINGWATER_VERSION","1.6.7"),
        rsparkling.spark.version =          Sys.getenv("SPARK_VERSION","1.6.2"))

if(identical(Sys.getenv("NOT_CRAN"), "true")) { # testthat::skip_on_cran
  test_check("rsparkling")
} else {
  cat("Skipping tests\n")
}
