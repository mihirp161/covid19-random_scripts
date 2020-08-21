# SAIL Labs
# ~Mihir

## Description:
##-------------
#* We can use this file to finalise Dr Nic's csv after we create all the data.
##-------------

options(scipen = 99999, warn = -1, stringsAsFactors = FALSE)
library(dplyr)
library(readr)
library(data.table)

#-------- read user data

setwd('./path_to_user_data/')

#filenames
files <- list.files( pattern="*.csv$")

#read files
temp <- lapply(files, function(x) readr::read_csv(x))
nic_data <- data.table::rbindlist(temp, fill = T) 

#-----------------------------------

#add the comeplete text column 
nic_data$complete_texts <- paste(nic_data$full_text, nic_data$extended_tweet.full_text, sep='_')

#remove the rows that begin with RT
nic_data <- nic_data[!grepl("^RT", nic_data$complete_texts), ]

#remove the rows that begin with NA_NA
nic_data <- nic_data[!grepl("^NA_NA", nic_data$complete_texts), ]

#remove the string NA
nic_data$complete_texts <- gsub('_NA|NA_', '',nic_data$complete_texts)

#--------------------------------------

#remove columns we don't need
nic_data <- subset(nic_data, select= -c(full_text, extended_tweet.full_text))


#write it to a file before we filter out
readr::write_excel_csv(nic_data, "dr_nic_data.csv")

#EOF