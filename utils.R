#-------------------------------------------------------------------------------------------------
# Common R Utilities
#-------------------------------------------------------------------------------------------------

library(dplyr)
library(readxl)

#-------------------------------------------------------------------------------------------------
# TRACELOG
#-------------------------------------------------------------------------------------------------
trace_file = "NA"
TRACELOG_INIT <- function(filename) {
  if (trace_file == "NA") {
    trace_file <<- filename
    write(sprintf("[%s] --> STARTING NEW RUN\n", Sys.time()), file=trace_file)
  }
}

TRACELOG <- function(message, NL=TRUE, stdoutput=TRUE) {
  if (stdoutput) {
    cat(message)
    if (NL) {
      cat("\n")
    }
  }
  write(message, file=trace_file, append=TRUE)
}

#-------------------------------------------------------------------------------------------------
# File handling
#-------------------------------------------------------------------------------------------------
file_exists <- function(fullpath_filename) {
  return(file.exists(fullpath_filename))
}

delete_file <- function(fullpath_filename) {
  if (file.exists(fullpath_filename)) { file.remove(fullpath_filename) }
  else return(TRUE)
}

#-------------------------------------------------------------------------------------------------
# File handling
#-------------------------------------------------------------------------------------------------
dump_warnings <- function() {
  TRACELOG("\nDUMPING ALL WARNINGS", TRUE, FALSE)
  TRACELOG("=============================================================================", TRUE, FALSE)
  TRACELOG(paste(warnings(), collapse = "\n"), TRUE, FALSE)
  TRACELOG("=============================================================================", TRUE, FALSE)
}


