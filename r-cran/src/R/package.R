getVersion <- function(sparkMajor) {
  content <- read.csv(url(paste0("http://h2o-release.s3.amazonaws.com/sparkling-water/spark-", sparkMajor, "/latest")), header = FALSE)
  as.vector(content[1, 1])
}

getLink <- function(sparkMajor) {
  paste0('install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-', sparkMajor, '/', getVersion(sparkMajor), '/R")')
}

spark_dependencies <- function(spark_version, scala_version, ...) {

  stop(paste0('You are using RSParkling installed from CRAN which is not maintained anymore. Please install latest RSparkling as:

  For Spark 2.4 -> ', getLink("2.4"), '
  For Spark 2.3 -> ', getLink("2.3"), '
  For Spark 2.2 -> ', getLink("2.2"), '
  For Spark 2.1 -> ', getLink("2.1")))
  spark_dependency(jars = c())
}

.onLoad <- function(libname, pkgname) {
  register_extension(pkgname)
}
