library(testthat)
library(dplyr)
library(sparklyr)
library(h2o)
library(rsparkling)

if (identical(Sys.getenv("NOT_CRAN"), "true")) { # testthat::skip_on_cran
    test_check("rsparkling")
} else {
    cat("Skipping Tests\n")
}
