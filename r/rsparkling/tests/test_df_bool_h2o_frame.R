library(testthat)
library(rsparkling)
library(dplyr)

#Test transformation of a spark data_frame of bools to an h2o frame of bools
test_df_bool_h2o_frame = function(){

  df = copy_to(sc,as.data.frame(t(c(TRUE,FALSE,TRUE,FALSE))),overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],1)
  expect_equal(df_hex[1,2],0)
  expect_equal(df_hex[1,3],1)
  expect_equal(df_hex[1,4],0)
}

#Create a spark connection
sc <- spark_connect(master = "local")

#Run test
test_df_bool_h2o_frame()