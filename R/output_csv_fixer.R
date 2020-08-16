# SAIL Labs
# ~Mihir

## Description:
##-------------
#* This file removes the usernames that have been already processed, copies all the botometer score csv
#* to a new directory. THis file also rename files with date so we know when the scores files were made.
##-------------


library(data.table)
library(dplyr)

#------------------------------------------- file reading and collapsing ----------------------------------------------------

# first read all the files in BOT_CSV folder (Give the path to folder, ending /)
setwd('./BOT_CSV/')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) data.table::fread(x, select = c("user_screen_name")))
botcsv_data <- data.table::rbindlist(temp, fill = T) #make a df

#create a directory
setwd('..') #goes back once
dir.create("./BOT_CSV_OLD/") #(Give path )

#----------------------------------- use this portion to rename files into current directory, if moving is done-------------
setwd('./BOT_CSV/')
#rename the file with date
file.rename(from = files, to = paste0(format(file.mtime(files),"%Y-%m-%d"),"_",files)) #in linux, do file.info(files)$mtime
#--------------------------------------------------------------------------------------------------------------------------
#overwite old naems
files <- list.files( pattern="*.csv$")

#copy all the exports file to a new folder name them accordingly to prevent deletion and overwritinh
file.copy(files,
          to = "../BOT_CSV_OLD/", recursive = T,
          overwrite = T, copy.mode = T, copy.date = F)
setwd('..')

#----------------------------------------------- rename all the files that were copied -----------------------------------------
#rename the files based on direcotry
setwd('./BOT_CSV_OLD/')

folder<-"./BOT_CSV_OLD/"

files<-list.files( pattern="*.csv$")

folder_name<- unlist(strsplit(folder,"/"))[length(unlist(strsplit(folder,"/")))]

#file.rename(from = paste0(folder,files),to = paste0(folder,folder_name,"_",files))
file.rename(from = files,to = paste0(folder_name,"_", files))

setwd('..') #move the working directory back, or give an specific path

message("Please don't delete the BOT_CSV_OLD directory, it contains the work you've already done.")
#--------------------------------------------- remake outputcsv file so you can carry on ---------------------------------------------

#read
outputcsv_file <- readr::read_csv("output.csv") #in linux, just to read.csv("output.csv",stringsAsFactors = F)
colnames(botcsv_data) <- "user_screen_name"
#now compare the two frame and remove the rows which are in 

outputcsv_file <- dplyr::anti_join(outputcsv_file,botcsv_data, by= "user_screen_name")

#write the file back
readr::write_csv(outputcsv_file, "output.csv") #in linux, do write.csv("output.csv")

#EOF