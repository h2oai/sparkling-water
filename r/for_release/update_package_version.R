updatePackageVersion <- function(packageLocation ="."){
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
  desc[vLine] <- paste0("Version: ", vFinal )
  
  # Update the actual DESCRIPTION file
  writeLines(desc, file.path(packageLocation, "DESCRIPTION"))
  
  # Return the updated version number to screen
  return(vFinal)
}

updatePackageVersion("../../rsparkling")