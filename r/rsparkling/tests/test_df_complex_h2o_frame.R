library(testthat)
library(rsparkling)
library(dplyr)

#Test transformation of a spark data_frame of complex types to an h2o frame of bools
test_df_complex_h2o_frame = function(){
  n = c(2)
  s = c("aa")
  b = c(TRUE)
  df_r = data.frame(n, s, b)
  df = copy_to(sc,df_r,overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],2)
  expect_equal(df_hex[1,2],"aa")
  expect_equal(df_hex[1,3],1)
}

#Create a spark connection
sc <- spark_connect(master = "local")

#Run test
test_df_complex_h2o_frame()