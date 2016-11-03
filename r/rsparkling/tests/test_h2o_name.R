# Test of transformations from dataframe to h2o frame and from h2o frame back to dataframe

library(testthat)
library(dplyr)
library(sparklyr)
library(h2o)
library(rsparkling)

# Testing passing name to a dataframe
test_h2o_name = function(){

   mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
   mtcars_hf_name <- as_h2o_frame(sc, mtcars_tbl, name = "frame1")

   expect_equal(h2o.getId(mtcars_hf_name), "frame1")


}

#Create a spark connection
sc <- spark_connect(master = "local")

#Run tests
test_h2o_name()
