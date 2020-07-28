# For USF SAIL
# ~Mihir

library(data.table)
library(readr)
library(dplyr)
library(rio)

#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('C:\\Users\\ThinkPad\\SpyderProjects\\JsonStuff\\100K_March_URL_GONE')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) data.table::fread(x, select = c("user.screen_name")))
data <- data.table::rbindlist(temp, fill = T) #make a df

setwd('..')

#remove duplicates
data <- unique(data)

#sample N users and write them to a file
readr::write_excel_csv(data[sample(.N, 100)], "meghan_users.csv")


#-----------------------------------
#user this portion if you want to reduce the sample csv file with annotation files

#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('C:\\Users\\ThinkPad\\SpyderProjects\\JsonStuff\\100K_March_URL_GONE')

full_file <- readr::read_csv("july_people_full_randomised.csv")
  
current_sam_file <- readr::read_csv("july_people_for_annotation_sample1.csv")

full_file <- dplyr::anti_join(full_file, current_sam_file, by='user_screen_name')

#overwrite the file
readr::write_excel_csv(full_file, "july_people_full_randomised.csv")



#---------------------------------------- merge the annoation to bot

#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('C:\\Users\\ThinkPad\\Desktop\\July Botometer USF DATA 1\\july mihir botometer')

#filenames
files <- list.files( pattern="*.csv$")

#read bot files
temp <- lapply(files, function(x) readr::read_csv(x))
bot_data <- data.table::rbindlist(temp, fill = T) #make a df

names(bot_data)[names(bot_data) == 'user_screen_name'] <- 'ScreenName'


setwd('C:\\Users\\ThinkPad\\Desktop\\July Botometer USF DATA 1')

#read the annoated file
anno_file <- rio::import_list("Round3MergedAnnotationsUpdated.xlsx", setclass = "tbl")


#--->now merge the dataframe
anno_tbl_filtered_w_bot <- dplyr::left_join(anno_file$CombinedAnnotation, bot_data, by= "ScreenName") # Or inner_join??

#put back the df in the sheet
anno_file$CombinedAnnotation <- anno_tbl_filtered_w_bot

rio::export(anno_file, file= "Round3MergedAnnotationsUpdated_botAttached.xlsx")
