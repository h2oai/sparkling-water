library(testthat)
library(dplyr)
library(sparklyr)
library(h2o)
library(rsparkling)

if (identical(Sys.getenv("NOT_CRAN"), "true")) { # testthat::skip_on_cran
    # Set Sparkling Water Water Jar location and version for tests
    options(rsparkling.sparklingwater.location = Sys.getenv("sparkling.assembly.jar"))
    options(rsparkling.sparklingwater.version = Sys.getenv("sparkling.water.version"))
    # Set Spark version for tests
    options(rsparkling.spark.version = Sys.getenv("spark.version"))
    test_check("rsparkling")
} else {
    cat("Skipping Tests\n")
}
