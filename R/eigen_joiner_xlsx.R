# SAIL Labs
# ~Mihir

## Description:
##-------------
#* joins the bot scores to vaccine related xlxs files
##-------------

options(scipen = 99999)
library(openxlsx)
library(data.table)
library(plyr)
library(readr)

#read the original gephi csv output file
xl_analysisFile <- openxlsx::read.xlsx("Vaccine.july.network.queries.xlsx")

#go to where expoerted bot csv files are (these are probably the one you get when you run populate_bot_fils.py)
setwd('./BOT_CSV_USC/')

#filenames
files <- list.files( pattern="*.csv$")

#read only all columns
temp <- lapply(files, function(x) readr::read_csv(x))
exports_data <- data.table::rbindlist(temp, fill = T) #make a df

setwd('..') #move the working directory back, or give an specific path

#renames first two columns
exports_data <- plyr::rename(exports_data, c("user_user_data_screen_name"= "user_screen_name",
                                             "user_user_data_id_str"= "user_id_str"))

xl_analysisFile <- plyr::rename(xl_analysisFile, c("Id"= "user_screen_name"))


#cut the main file table based by 2000
xl_analysisFile <- xl_analysisFile[1:nrow(read.csv("output_test_en.csv")), ]

#check which users weren't found
#filteredcsv_data <- xl_analysisFile[!xl_analysisFile$user_screen_name %in% exports_data$user_screen_name, , drop = FALSE]

#join the files by leaving NA if no match
xl_analysisFile2 <- plyr::join(xl_analysisFile, exports_data, type="left")

#write
openxlsx::write.xlsx(xl_analysisFile2, "Vaccine.july.network.queries_WITHBOTS.xlsx")

#EOF