library(testthat)
library(rsparkling)
library(dplyr)

#Test transformation from dataframe to h2o frame
test_df_to_h2o_frame = function(){

   mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
   mtcars_hf <- as_h2o_frame(sc, mtcars_tbl)

   expect_equal(as.data.frame(tally(mtcars_tbl))$n,nrow(mtcars_hf))
   expect_equal(ncol(mtcars_tbl),ncol(mtcars_hf))
   expect_equal(colnames(mtcars_tbl),colnames(mtcars_hf))


}

#Create a spark connection
sc <- spark_connect(master = "local")

#Run test
test_df_to_h2o_frame()