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

next_from_existing_table <- function(release_table, spark_version, h2o_version, h2o_name, h2o_build){
    latest <- data.frame( Spark_Version = spark_version,
    Sparkling_Water_Version = paste(spark_version, ".", as.numeric(unlist(strsplit(toString(release_table[1,2]), "\\."))[3]) + 1, collapse="", sep=""),
    H2O_Version = c(h2o_version),
    H2O_Release_Name = c(h2o_name),
    H2O_Release_Patch_Number = c(h2o_build), stringsAsFactors=FALSE)

    return(latest)
}


h2o_release_table <- function(tables_dir, h2o_version, h2o_name, h2o_build){

    table_2_3 <- read.table(file=paste(tables_dir, "table_2_3.txt", sep="", collapse=""))
    table_2_2 <- read.table(file=paste(tables_dir, "table_2_2.txt", sep="", collapse=""))
    table_2_1 <- read.table(file=paste(tables_dir, "table_2_1.txt", sep="", collapse=""))

    next_version_2_3 <- next_from_existing_table(table_2_3, "2.3", h2o_version, h2o_name, h2o_build)
    next_version_2_2 <- next_from_existing_table(table_2_2, "2.2", h2o_version, h2o_name, h2o_build)
    next_version_2_1 <- next_from_existing_table(table_2_1, "2.1", h2o_version, h2o_name, h2o_build)
    final_2_3 <- rbind(next_version_2_3, table_2_3)
    final_2_2 <- rbind(next_version_2_2, table_2_2)
    final_2_1 <- rbind(next_version_2_1, table_2_1)

    write.table(final_2_3, file=paste(tables_dir, "table_2_3.txt", sep="", collapse=""))
    write.table(final_2_2, file=paste(tables_dir, "table_2_2.txt", sep="", collapse=""))
    write.table(final_2_1, file=paste(tables_dir, "table_2_1.txt", sep="", collapse=""))

    return(rbind(final_2_3, final_2_2, final_2_1))
}

generate_sys_data <- function(tables_dir, h2o_version, h2o_name, h2o_build){
    release_table <- h2o_release_table(tables_dir, h2o_version, h2o_name, h2o_build)
    devtools::use_data(release_table, internal = TRUE, overwrite = TRUE)
}

write_release_table <- function(destination, tables_dir, h2o_version, h2o_name, h2o_build){
    release_table <- h2o_release_table(tables_dir, h2o_version, h2o_name, h2o_build)
    write.csv(release_table, file=destination, row.names = FALSE)
}

getCurrentVersion <- function(packageLocation = "."){
    # Read DESCRIPTION file
    desc <- readLines(file.path(packageLocation, "DESCRIPTION"))

    # Find the line where the version is defined
    vLine <- grep("^Version\\:", desc)

    # Extract version number
    vNumber <- gsub("^Version\\:\\s*", "", desc[vLine])

    # Split the version number into two; a piece to keep, a piece to increment
    versionNumber <- strsplit(vNumber, "\\.")[[1]]
    versionParts <- length(versionNumber)
    vNumberUpdate <- versionNumber[versionParts]

    # Replace old version number with new one (increment by 1)
    oldVersion <- as.numeric(vNumberUpdate)
    oldVersion
}

updatePackageVersion <- function(packageLocation = "."){
    # Read DESCRIPTION file
    desc <- readLines(file.path(packageLocation, "DESCRIPTION"))

    # Find the line where the version is defined
    vLine <- grep("^Version\\:", desc)

    # Extract version number
    vNumber <- gsub("^Version\\:\\s*", "", desc[vLine])

    # Split the version number into two; a piece to keep, a piece to increment
    versionNumber <- strsplit(vNumber, "\\.")[[1]]
    versionParts <- length(versionNumber)
    vNumberKeep <- paste(versionNumber[1:(versionParts-1)], sep= "", collapse= ".")
    vNumberUpdate <- versionNumber[versionParts]

    # Replace old version number with new one (increment by 1)
    oldVersion <- as.numeric(vNumberUpdate)
    newVersion <- oldVersion + 1

    # Build final version number
    vFinal <- paste(vNumberKeep, newVersion, sep = ".")

    # Update DESCRIPTION file (in R)
    desc[vLine] <- paste0("Version: ", vFinal)

    # Update the actual DESCRIPTION file
    writeLines(desc, file.path(packageLocation, "DESCRIPTION"))

    # Return the updated version number to screen
    return(vFinal)
}