options(scipen = 9999999, digits = 22)
library(dplyr)
library(readxl)
library(readr)
library(bit64)



# SAIL Labs
# ~Mihir

## Description:
##-------------
#* This file will read all the csv, select the Screenid and keeps the unique userids only.
#*  Then you can specify people and number of rows you want to keep in each file. It is meant to
#*  help with botometer.
##-------------


library(data.table)

# first read all the files in a folder (Give the path to folder (....), ending /)
setwd('./path_to_files_here/')

#filenames
files <- list.files( pattern="*csv$")

#read only columns that we want
temp <- lapply(files, function(x) data.table::fread(x, select = c("Screenid")))
data <- data.table::rbindlist(temp, fill = T) #make a df


#remove the duplicates userids
n_original <-  data[!duplicated(data$Screenid), ]

#sample df with probablity sample without replacements
set.seed(786)
rand_data <- data.frame(Screenid= sample( bit64::as.integer64(n_original$Screenid), nrow(n_original), replace = F), stringsAsFactors = F)



# Here I want ~2500 rows per file until last row is reached
# this method forces the execution from innermost, outer () opttional
groups <- (split(rand_data, (seq(nrow(rand_data))-1) %/% 2537))
people <- c("1_user", "2_user", "3_user", "4_user", "5_user")

#loop through end of each split, and write file with i
for (i in seq_along(groups)) {
  readr::write_csv(groups[[i]], paste0("output_", people[i], ".csv")) 
}
