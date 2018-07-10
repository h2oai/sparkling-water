library(jsonlite)

get_build_vectors <- function(spark_major_minor_version){
    latest <- fromJSON(sprintf("http://s3.amazonaws.com/h2o-release/sparkling-water/rel-%s/latest", spark_major_minor_version))
    all_build_versions <- seq(0, latest)
}


get_release_table_for <- function(spark_major_minor_version) {

    spark_version = c()
    sw_version = c()
    h2o_version = c()
    h2o_name = c()
    h2o_patch_version = c()

    build_versions <- get_build_vectors(spark_major_minor_version)
    for (i in 1:length(build_versions)) {
        build_info <- fromJSON(sprintf("https://s3.amazonaws.com/h2o-release/sparkling-water/rel-%s/%s/buildinfo.json", spark_major_minor_version, build_versions[i]))
        spark_version[i] <- spark_major_minor_version
        sw_version[i] <- sprintf("%s.%s",spark_major_minor_version, build_versions[i])
        h2o_version[i ] <- build_info$h2o_project_version
        h2o_name[i] <- build_info$h2o_branch_name
        h2o_patch_version[i] <- build_info$h2o_build_number
    }

    data.frame( Spark_Version = spark_version,
    Sparkling_Water_Version = rev(sw_version),
    H2O_Version = rev(h2o_version),
    H2O_Release_Name = rev(h2o_name),
    H2O_Release_Patch_Number = rev(h2o_patch_version))
}

h2o_release_table <- function(){
    return(rbind(get_release_table_for("2.3"), get_release_table_for("2.2"), get_release_table_for("2.1")))
}

release_table <- h2o_release_table()
devtools::use_data(release_table, internal = TRUE, overwrite = TRUE)
