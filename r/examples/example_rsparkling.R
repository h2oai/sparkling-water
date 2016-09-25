# Example of rsparkling for machine learning:

library(rsparkling) 

# If requred, Spark can be installed via the sparklyr command:
spark_install(version = "1.6.2")

# Create a spark connection
sc <- spark_connect(master = "local")

# Inspect the H2OContext for our Spark connection
# This will also start an H2O cluster
h2o_context(sc)

# We can also view the H2O Flow web UI:
h2o_flow(sc)


# H2O with Spark DataFrames

# Let's copy the mtcars dataset to to Spark as an example:
library(dplyr)
mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
mtcars_tbl

# Convert the Spark DataFrame into an H2OFrame
mtcars_hf <- as_h2o_frame(sc, mtcars_tbl)
mtcars_hf


# Split the mtcars H2O Frame into train & test sets
splits <- h2o.splitFrame(mtcars_hf, ratios = 0.7, seed = 1)
nrow(splits[[1]])  # nrows in train
nrow(splits[[2]])  # nrows in test

# Train an H2O Gradient Boosting Machine (GBM)
# And perform 3-fold cross-validation via `nfolds`
y <- "mpg"
x <- setdiff(names(mtcars_hf), y)
fit <- h2o.gbm(x = x, 
               y = y, 
               training_frame = splits[[1]], 
               nfolds = 3, 
               min_rows = 1,
               seeed = 1)

# Evaluate 3-fold cross-validated model performance:
h2o.performance(fit, xval = TRUE)

# As a comparison, we can evaluate performance on a test set
h2o.performance(fit, newdata = splits[[2]])

# Note: Since this is a very small data problem, 
# we see a reasonable difference between CV and 
# test set metrics


# Now, generate the predictions (as opposed to metrics)
pred_hf <- h2o.predict(fit, newdata = splits[[2]])
pred_hf

# If we want these available in Spark:
pred_sdf <- as_spark_dataframe(sc, pred_hf)
pred_sdf


# Other useful functions:

# Inspect Spark log directly
spark_log(sc, n = 20)


# Now we disconnect from Spark, this will result in the H2OContext being stopped as 
# well since it's owned by the spark shell process used by our Spark connection:
spark_disconnect(sc)


