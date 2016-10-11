library(testthat)
library(rsparkling)
library(dplyr)

#Test transformation of a spark data_frame of int types to an h2o frame of bools
test_df_int_h2o_frame = function(){

  df = copy_to(sc,as.data.frame(t(c(1,125,1778))),overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],1)
  expect_equal(df_hex[1,2],125)
  expect_equal(df_hex[1,3],1778)
}

#Create a spark connection
sc <- spark_connect(master = "local")

#Run test
test_df_int_h2o_frame()