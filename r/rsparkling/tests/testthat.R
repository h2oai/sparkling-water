if(requireNamespace("testthat", quietly = TRUE)){
    library(testthat)
    library(rsparkling)
    library(dplyr)
    library(sparklyr)
    test_check("rsparkling")
}
