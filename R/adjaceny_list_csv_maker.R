# SAIL Labs
# ~Mihir

## Description:
##-------------
#* This script makes an edgelist csv file for gephi software.
#* Reads bunch of csvs from a directory, while reading, it only selects
#* username, and retweet username.
#* Then We only keep unique pairs and discard the rest. We get their frequencies
#* and assign direc or undirect tags, and we put them in a csv file.
##-------------

library(tidyverse)
library(data.table)
library(igraph)
library(rgexf)
library(dplyr)
#-------------------------------------------------- file reading and collapsing -----------------------------------

# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('/your_directory/')

#filenames 
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) data.table::fread(x, select = c("user.screen_name", "retweeted_status.user.screen_name")))
data <- data.table::rbindlist(temp, fill = T) #make a df

#------------------------------------------------------ make adjaceny list ------------------------------------------
#currently we will make csv files, focus on .gefx file later on.

#remove all rows with space in the retweets
#data <- data[data$retweeted_status.user.screen_name != "", ]
data <- data[!is.na(data$retweeted_status.user.screen_name), ]

#get the frequencies
edgelist2 <- dplyr::summarise(dplyr::group_by(data,user.screen_name, retweeted_status.user.screen_name),count =n())

#Sort descending and remove duplicates pair just in case
decreasing_df <- edgelist2[order(edgelist2$count, decreasing = TRUE), ]  
decreasing_df <- decreasing_df[!duplicated(decreasing_df[c(1,2)]), ]

# cut off range by numbers of rows (uncomment when asked)
# decreasing_df <- decreasing_df[1:50000, ]

Type <- rep("Directed", nrow(decreasing_df))

#assign tags, in this case edges are directred, meaning username is connected to the retweeted user name
decreasing_df$Type <- Type

#change column names
colnames(decreasing_df) <- c("Source", "Target", "Weight","Type")

decreasing_df <- decreasing_df[decreasing_df$Weight >= 6, ] # new cutoff range, filter everything below 6 weight

# organize the columns so gephi recogize them
decreasing_df <- decreasing_df[,c("Source", "Target", "Type", "Weight")]

#give the directory where you want to save the file
setwd('/your_directory/')

readr::write_csv(decreasing_df,"edgelist_file_name.csv")

#EOF
