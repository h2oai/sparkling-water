library(rsparkling)
context("Test transformations of H2O frames and Spark frames in rsparkling")

test_that("Test transformation from h2o frame to data frame", {
  sc <- spark_connect(master = "local")
  df = copy_to(sc,as.data.frame(t(c(1,2,3,4,"A"))))
  df_hex = as_h2o_frame(sc,df)
  df_tbl = as_spark_dataframe(sc,df_hex)

  expect_equal(as.data.frame(tally(df_tbl))$n,nrow(df_hex))
  expect_equal(ncol(df_tbl),ncol(df_hex))
  expect_equal(colnames(df_tbl),colnames(df_hex))


})

test_that("Test transformation of a spark data_frame of bools to an h2o frame of bools", {
  sc <- spark_connect(master = "local")
  df = copy_to(sc,as.data.frame(t(c(TRUE,FALSE,TRUE,FALSE))),overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],1)
  expect_equal(df_hex[1,2],0)
  expect_equal(df_hex[1,3],1)
  expect_equal(df_hex[1,4],0)


})

test_that("Test transformation of a spark data_frame of complex types to an h2o frame of complex types", {
  sc <- spark_connect(master = "local")
  n = c(2)
  s = c("aa")
  b = c(TRUE)
  df_r = data.frame(n, s, b)
  df = copy_to(sc,df_r,overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],2)
  expect_equal(df_hex[1,2],"aa")
  expect_equal(df_hex[1,3],1)


})

test_that("Test transformation of a spark data_frame of float types to an h2o frame of floats", {
  sc <- spark_connect(master = "local")
  df = copy_to(sc,as.data.frame(t(c(1.5,1.3333333333,178.5555))),overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],1.5)
  expect_equal(df_hex[1,2],1.3333333333)
  expect_equal(df_hex[1,3],178.5555)


})

test_that("Test transformation of a spark data_frame of int types to an h2o frame of ints", {
  sc <- spark_connect(master = "local")
  df = copy_to(sc,as.data.frame(t(c(1,125,1778))),overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],1)
  expect_equal(df_hex[1,2],125)
  expect_equal(df_hex[1,3],1778)

})

test_that("Test transformation of a spark data_frame of str types to an h2o frame of str", {
  sc <- spark_connect(master = "local")
  df = copy_to(sc,as.data.frame(t(c("A","B","C"))),overwrite = TRUE)
  df_hex = as_h2o_frame(sc,df)

  expect_equal(df_hex[1,1],"A")
  expect_equal(df_hex[1,2],"B")
  expect_equal(df_hex[1,3],"C")

})

test_that("Test transformation from dataframe to h2o frame", {
  sc <- spark_connect(master = "local")
   mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
   mtcars_hf <- as_h2o_frame(sc, mtcars_tbl)

   expect_equal(as.data.frame(tally(mtcars_tbl))$n,nrow(mtcars_hf))
   expect_equal(ncol(mtcars_tbl),ncol(mtcars_hf))
   expect_equal(colnames(mtcars_tbl),colnames(mtcars_hf))

})


test_that("Test transformation from dataframe to h2o frame", {
   sc <- spark_connect(master = "local")
   mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
   mtcars_hf_name <- as_h2o_frame(sc, mtcars_tbl, name = "frame1")

   expect_equal(h2o::h2o.getId(mtcars_hf_name), "frame1")

})

