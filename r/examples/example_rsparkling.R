# Example of rsparkling:

# Install sparklyr
#install.packages("devtools")
#devtools::install_github("rstudio/sparklyr")

library(sparklyr)
#spark_install(version = "1.6.2")
library(devtools)
#devtools::install_github("h2oai/sparkling-water/r/rsparkling")
library(rsparkling) 

# Create a spark context
sc <- spark_connect(master = "local")

# Inspect hte H2OContext for our Spark connection
h2o_context(sc)

# We can also view the H2O Flow web UI:
h2o_flow(sc)


# H2O with Spark DataFrames

# Let's copy the mtcars dataset to to Spark so we can access it from Sparkling Water:
library(dplyr)
mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
mtcars_tbl

# Convert the Spark DataFrame into an H2OFrame
mtcars_hf <- h2o_frame(mtcars_tbl)
mtcars_hf


# Now we disconnect from Spark, this will result in the H2OContext being stopped as 
# well since it's owned by the spark shell process used by our Spark connection:
spark_disconnect(sc)


