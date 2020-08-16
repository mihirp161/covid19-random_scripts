# SAIL Labs
# ~Mihir

## Description:
##-------------
#* Script containing three individual parts. First one subsets users from selected month. 
#* Second one is to reduce the size of a full csv if you took sample out of it. And then the
#* last portion cab be used to merge annotations and botometer files.
##-------------

library(data.table)
library(readr)
library(dplyr)
library(rio)

#-------------------------- portion to subset users from monthly data---------------------------------

#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('/file_path_here/')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) data.table::fread(x, select = c("user.screen_name")))
data <- data.table::rbindlist(temp, fill = T) #make a df

setwd('..')

#remove duplicates
data <- unique(data)

#sample N users and write them to a file
readr::write_excel_csv(data[sample(.N, 100)], "some_users.csv")

#----------------------------------- reduce the portion for sample csv -----------------------------
#use this portion if you want to reduce the sample csv file with annotation files

#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('/file_path_here/')

full_file <- readr::read_csv("full_randomised.csv")
  
current_sam_file <- readr::read_csv("sample_annotation.csv")

#keep the ones that don't match
full_file <- dplyr::anti_join(full_file, current_sam_file, by='user_screen_name')

#overwrite the file
readr::write_excel_csv(full_file, "full_randomised.csv")


#---------------------------------------- merge the annoation to bot---------------------------------
#use this portion if you want to merge botometer and annotation files

#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('/file_path_here/')

#filenames
files <- list.files( pattern="*.csv$")

#read bot files
temp <- lapply(files, function(x) readr::read_csv(x))
bot_data <- data.table::rbindlist(temp, fill = T) #make a df

names(bot_data)[names(bot_data) == 'user_screen_name'] <- 'ScreenName'


setwd('/file_path_here/')

#read the annoated file
anno_file <- rio::import_list("annotation_file.xlsx", setclass = "tbl")


#--->now merge the dataframe
anno_tbl_filtered_w_bot <- dplyr::left_join(anno_file$CombinedAnnotation, bot_data, by= "ScreenName") 

#put back the df in the sheet
anno_file$CombinedAnnotation <- anno_tbl_filtered_w_bot

rio::export(anno_file, file= "annotation_file_botAttached.xlsx")

#EOF
