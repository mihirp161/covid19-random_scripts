library(data.table)
library(readr)

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
