# SAIL Labs
# ~Mihir

## Description:
##-------------
#* Reads bunch of csvs and only keeps the users who are from Florida.
##-------------

options(scipen = 999)

library(data.table)
library(readr)

#------------------------ read the files for location ------------------------------------------
setwd('./file_path/')

#filenames
files <- list.files( pattern="*.csv$")

#read
temp <- lapply(files, function(x) readr::read_csv(x))

loco_data <- data.table::rbindlist(temp, fill = T) #make a df

loco_data <- loco_data[,c("tweet.id_str", "place.full_name")]
#create a directory
setwd('..') #goes back once

#change the column names
colnames(loco_data) <- c("id_str", "place.full_name")

#reduced the data.table to Fl, and Florida only
loco_subbed <- loco_data[Reduce(`|`, Map(`%like%`, list(place.full_name), c(', FL', 'Florida, '))),]

rm(loco_data)
#------------------------ read the files for queried csv ------------------------------------------

setwd('./file_path/')

#filenames
files <- list.files( pattern="*.csv$")

#read
temp <- lapply(files, function(x) readr::read_csv(x))

filter_data <- data.table::rbindlist(temp, fill = T) #make a df

#create a directory
setwd('..') #goes back once

#------------------------ reduce the dataset to Florida Specific only ------------------------------

#merge two df with tweet id_str
tuc_mary_csv <- merge(filter_data, loco_subbed, by="id_str")

#then write the data to file
readr::write_excel_csv(tuc_mary_csv, "florida_data.csv")

#EOF