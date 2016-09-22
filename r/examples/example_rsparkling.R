# Example of rsparkling:

library(sparklyr)
library(h2o)
library(rsparkling) 

# Create a spark context
sc <- spark_connect(master = "local")

# Inspect the H2OContext for our Spark connection
# This will also start an H2O cluster
h2o_context(sc)

# We can also view the H2O Flow web UI:
h2o_flow(sc)


# H2O with Spark DataFrames

# Let's copy the mtcars dataset to to Spark so we can access it from Sparkling Water:
library(dplyr)
mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
mtcars_tbl

# Convert the Spark DataFrame into an H2OFrame
mtcars_hf <- as_h2o_frame(mtcars_tbl)
mtcars_hf


# Train an H2O Gradient Boosting Machine (GBM)
y <- "mpg"
x <- setdiff(names(mtcars_hf), y)
fit <- h2o.gbm(x = x, 
               y = y, 
               training_frame = mtcars_hf, 
               nfolds = 2, 
               min_rows = 1)

prediction_hf <- h2o.predict(fit, mtcars_hf)

prediction_tbl <- as_spark_dataframe(prediction_hf, sc)
prediction_tbl


# Inspect Spark log directly
spark_log(sc, n = 100)


# Now we disconnect from Spark, this will result in the H2OContext being stopped as 
# well since it's owned by the spark shell process used by our Spark connection:
spark_disconnect(sc)


