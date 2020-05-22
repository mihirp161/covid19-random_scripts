#For USF SAIL
#~Mihir 

library(tidyverse)
library(data.table)
library(igraph)
library(rgexf)
library(dplyr)
#-------------------------------------------------- file reading and collapsing -----------------------------------

# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('./RawCSV_firstBatch/')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) data.table::fread(x, select = c("screenNames", "retweetUserScreenName")))
data <- data.table::rbindlist(temp, fill = T) #make a df

setwd('..') #move the working directory back, or give an specific path

readr::read_csv(data, "username_text.csv")

#remove all rows with space in the retweets
data <- data[data$retweetUserScreenName != "", ]
#--------------------------------------------------- removing duplicates ------------------------------------------- 
#remove duplicates pairs (compitable for both undirected and dirtected edges!!)             
data <- data[!duplicated(data), ]
#------------------------------------------------------ make adjaceny list ------------------------------------------
#currently we will make csv files, focus on gefx file later on.
               
edgelist2 <- dplyr::summarise(dplyr::group_by(data,screenNames, retweetUserScreenName),count =n())

#Sort descending and remove duplicates pair just in case
decreasing_df <- edgelist2[order(edgelist2$count, decreasing = TRUE), ]  
decreasing_df <- decreasing_df[!duplicated(decreasing_df[c(1,2)]), ]

# cut off range by numbers of rows
decreasing_df <- decreasing_df[1:10000, ]
colnames(decreasing_df) <- c("Source", "Target", "Weight")

readr::write_csv(decreasing_df,"csv_eddgeList.csv")
               
#EOF
