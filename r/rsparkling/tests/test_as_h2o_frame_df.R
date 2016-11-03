library(testthat)
library(rsparkling)
library(dplyr)

#Test transformation from h2o frame to data frame
test_as_h2o_frame_df = function(){

  df = copy_to(sc,as.data.frame(t(c(1,2,3,4,"A"))))
  df_hex = as_h2o_frame(sc,df)
  df_tbl = as_spark_dataframe(sc,df_hex)

  expect_equal(as.data.frame(tally(df_tbl))$n,nrow(df_hex))
  expect_equal(ncol(df_tbl),ncol(df_hex))
  expect_equal(colnames(df_tbl),colnames(df_hex))


}

#Create a spark connection
sc <- spark_connect(master = "local")

#Run test
test_as_h2o_frame_df()