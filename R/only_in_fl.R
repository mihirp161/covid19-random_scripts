#For USF SAIL
# ~M
options(scipen = 999)

library(data.table)
library(readr)
library(dplyr)

#------------------------ read the files for location ------------------------------------------
#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/locationr/july/')
setwd('./locationr/')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))

loco_data <- data.table::rbindlist(temp, fill = T) #make a df

loco_data <- loco_data[,c("tweet.id_str", "place.full_name")]
#create a directory
setwd('..') #goes back once

#change the column names
colnames(loco_data) <- c("id_str", "place.full_name")

#reduced the data.table to Fl, and Florida only ---- This is for Florida only tweet
#loco_subbed <- loco_data[Reduce(`|`, Map(`%like%`, list(place.full_name), c(', FL', 'Florida, '))),]


# get all the state abbrevioations and full name, make them up so they match 
x<- c(paste0(", ",state.abb), paste0(state.name,", "), ", USA")

# check if they match to full names ---- This is for USA only tweets
loco_subbed <- loco_data[Reduce(`|`, Map(`%like%`, list(place.full_name), x)),]


rm(loco_data)
#------------------------ read the files for queried csv ------------------------------------------

#setwd('/work_bgfs/m/mkpatel/SCRIPT/filtered_new_query_mask/july/')
setwd('./tweetr/')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))

filter_data <- data.table::rbindlist(temp, fill = T) #make a df

#create a directory
setwd('..') #goes back once

#------------------------ reduce the dataset to Florida Specific only ------------------------------

#merge two df with tweet id_str
tuc_mary_csv <- merge(filter_data, loco_subbed, by="id_str")

#then write the data to file
readr::write_excel_csv(tuc_mary_csv, "data_usa_or_fl.csv")

#EOF