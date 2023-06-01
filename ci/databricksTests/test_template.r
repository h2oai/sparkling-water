install.packages("sparklyr")
install.packages("RCurl")
library(sparklyr)
library(testthat)
install.packages("SUBST_H2O_PATH", repos = NULL, type="source")
install.packages("SUBST_RSPARKLING_PATH", repos = NULL, type="source")
library(rsparkling)
sc <- spark_connect(method = "databricks")
hc <- H2OContext.getOrCreate()

expect_equal(length(invoke(hc$jhc, "getH2ONodes")), 3)

# Test conversions
df <- as.data.frame(t(c(1, 2, 3, 4, "A")))
sdf <- copy_to(sc, df, overwrite = TRUE)
hc <- H2OContext.getOrCreate()
hf <- hc$asH2OFrame(sdf)
sdf2 <- hc$asSparkFrame(hf)

expect_equal(sdf_nrow(sdf2), nrow(hf))
expect_equal(sdf_ncol(sdf2), ncol(hf))
expect_true(all(colnames(sdf2)==colnames(hf)))

spark_disconnect(sc)
