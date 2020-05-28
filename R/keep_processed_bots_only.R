# FOR USF SAIL
# ~Mihir

library(data.table)

#------------------------------------------- file reading and collapsing ----------------------------------------------------

# first read all the files in BOT_CSV folder (Give the path to folder, ending /)
setwd('./BOT_CSV/')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) data.table::fread(x))
botcsv_data <- data.table::rbindlist(temp, fill = T) #make a df

#create a directory
setwd('..') #goes back once
dir.create("./USC_PROCESSED_ACCOUNTS_MOVE_TO_CIRCE/") #(Give path )

setwd('./BOT_CSV/')

#----------------------------------- use this portion to rename files into current directory, if moving is done-------------
#rename the file with date
file.rename(from = files, to = paste0(format(file.mtime(files),"%Y-%m-%d"),"_",files)) #in linux, do file.info(files)$mtime

setwd('..') #goes back once
#---------------------------------------- remake the bot file so you can carry on with the rest ---------------------------
#read
outputcsv_file <- read.csv("output_mihir.csv", stringsAsFactors = F) #in linux, just to read.csv("output.csv",stringsAsFactors = F)
outputcsv_file <- data.frame(outputcsv_file[,-1], stringsAsFactors = F)
colnames(outputcsv_file) <- "user_screen_name"

#now compare the two frame and copy all the bot scores to a new file

outputcsv_file <- merge(outputcsv_file, botcsv_data, by.x='user_screen_name', by.y='user_screen_name', all.y  =TRUE)

#apply(is.na(outputcsv_file),2,sum)

#write the file back
write.csv(outputcsv_file, "usc_users_processed.csv", row.names = F) #in linux, do write.csv("output.csv")

#------------------------------------------------- move the file to a new folder or somethig --------------------------------------


#copy all the exports file to a new folder name them accordingly to prevent deletion and overwritinh
file.copy("usc_users_processed.csv",
          to = "./USC_PROCESSED_ACCOUNTS_MOVE_TO_CIRCE/", recursive = T,
          overwrite = T, copy.mode = T, copy.date = F)
setwd('..')
