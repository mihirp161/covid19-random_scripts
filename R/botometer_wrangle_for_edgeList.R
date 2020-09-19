# SAIL Labs
# ~Mihir

## Description:
##-------------
#* If you need to run botometer on users from edgelist file, then use this file.
#* This will also create distributable files so N number of people can run the botometer. 
##-------------

library(readr)
library(data.table)
library(dplyr)
library(tidyr)

# read edgelist file
edgelist_file_ori <- data.table:::fread("edgelist_file.csv", select = c("Source", "Target"))

#move source under target
edgelist_file_ori <- tidyr::pivot_longer(edgelist_file_ori, Source:Target)

#rename to match the old botometer python script. This way less things needs to be edited out
colnames(edgelist_file_ori) <- c("category", "user_screen_name")

#save the original files
readr::write_excel_csv(edgelist_file_ori, "pre_botometer_file.csv")
  
#remove the duplicates
edgelist_file_ori <- edgelist_file_ori[!duplicated(edgelist_file_ori$user_screen_name), ]

# this method forces the execution from innermost, outer () opttional
groups <- (split(edgelist_file_ori, (seq(nrow(edgelist_file_ori))-1) %/% 16196))
people <- c("Name")

#loop through end of each split, and write file with i
for (i in seq_along(groups)) {
  write.csv(groups[[i]], paste0("edgeList_input_", people[i], ".csv")) 
}

#EOF