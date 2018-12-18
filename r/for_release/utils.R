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
    print(nrow(release_table))
    if (nrow(release_table) == 1) { # Just header
        first <- data.frame( Spark_Version = spark_version,
        Sparkling_Water_Version = paste(spark_version, ".", "1", collapse="", sep=""),
        H2O_Version = c(h2o_version),
        H2O_Release_Name = c(h2o_name),
        H2O_Release_Patch_Number = c(h2o_build), stringsAsFactors=FALSE)

        return(first)
    } else {
        latest <- data.frame( Spark_Version = spark_version,
        Sparkling_Water_Version = paste(spark_version, ".", as.numeric(unlist(strsplit(toString(release_table[1,2]), "\\."))[3]) + 1, collapse="", sep=""),
        H2O_Version = c(h2o_version),
        H2O_Release_Name = c(h2o_name),
        H2O_Release_Patch_Number = c(h2o_build), stringsAsFactors=FALSE)

        return(latest)
    }
}

rbind_with_previous_table <- function(new_table, old_table) {
    if (nrow(old_table) == 1){ # Just header
        return(new_table)
    } else {
        return(rbind(new_table, old_table))
    }
}

update_release_table <- function(tables_dir, spark_version, h2o_version, h2o_name, h2o_build){
    out_file <- paste("table_", spark_version, ".txt", collapse="", sep="")
    table <- read.table(file=paste(tables_dir, out_file, sep="", collapse=""))
    next_version <- next_from_existing_table(table, gsub("_", ".", spark_version), h2o_version, h2o_name, h2o_build)
    final <- rbind_with_previous_table(next_version, table)
    write.table(final, file=paste(tables_dir, out_file, sep="", collapse=""))
}

update_release_tables <- function(tables_dir, h2o_version, h2o_name, h2o_build){
    for (spark_version in c("2_4", "2_3", "2_2", "2_1")){
        update_release_table(tables_dir, spark_version, h2o_version, h2o_name, h2o_build)
    }
}

h2o_release_table <- function(tables_dir){
    table_2_4 <- read.table(file=paste(tables_dir, "table_2_4.txt", sep="", collapse=""))
    table_2_3 <- read.table(file=paste(tables_dir, "table_2_3.txt", sep="", collapse=""))
    table_2_2 <- read.table(file=paste(tables_dir, "table_2_2.txt", sep="", collapse=""))
    table_2_1 <- read.table(file=paste(tables_dir, "table_2_1.txt", sep="", collapse=""))

    return(rbind(table_2_4, table_2_3, table_2_2, table_2_1))
}

generate_sys_data <- function(tables_dir){
    release_table <- h2o_release_table(tables_dir)
    usethis::use_data(release_table, internal = TRUE, overwrite = TRUE)
}

write_release_table <- function(destination, tables_dir){
    release_table <- h2o_release_table(tables_dir)
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