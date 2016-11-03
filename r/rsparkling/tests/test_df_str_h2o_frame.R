library(testthat)
library(rsparkling)
library(dplyr)

#Test transformation of a spark data_frame of str types to an h2o frame of bools
test_df_str_h2o_frame = function(){

  df = copy_to(sc,as.data.frame(t(c("A","B","C"))),overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],"A")
  expect_equal(df_hex[1,2],"B")
  expect_equal(df_hex[1,3],"C")
}

#Create a spark connection
sc <- spark_connect(master = "local")

#Run test
test_df_str_h2o_frame()