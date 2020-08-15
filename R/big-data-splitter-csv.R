# SAIL Labs
# ~Mihir

## Description:
##-------------
#* Splits giant csv into multiple parts based on n rows.
##-------------

# R method to make multiple csvs based on Rows.

library(tidyverse)

#Read in the original csv file
bigdf_data <- readr::read_csv("xyz.csv") 

# Here I want 60000 rows per file until last row is reached
# this method forces the execution from innermost, outer () opttional
groups <- (split(bigdf_data, (seq(nrow(bigdf_data))-1) %/% 60000))

#loop through end of each split, and write file with i
for (i in seq_along(groups)) {
  write.csv(groups[[i]], paste0("bigdf_data_output_file", i, ".csv")) 
}
#Output: all the files will be in the current directory. 

#EOF