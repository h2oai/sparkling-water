library(testthat)
library(rsparkling)
library(dplyr)

#Test transformation of a spark data_frame of float types to an h2o frame of bools
test_df_float_h2o_frame = function(){

  df = copy_to(sc,as.data.frame(t(c(1.5,1.3333333333,178.5555))),overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],1.5)
  expect_equal(df_hex[1,2],1.3333333333)
  expect_equal(df_hex[1,3],178.5555)
}

#Create a spark connection
sc <- spark_connect(master = "local")

#Run test
test_df_float_h2o_frame()