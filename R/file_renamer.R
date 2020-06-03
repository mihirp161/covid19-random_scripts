#For USF SAIL
# ~Mihir

#------------> DON'T RUN ALL!!!, It is meant to run manually by you!!! <------------ #

#------UPDATE THESE----------#
TYPE_NAME_HERE <- "mihir"
WHICH_DATASET <- "USC"
FILE_CONTAINS <- "btscr"
#---------------------------#

#----------------------------------------------------------- Part 1 -------------------------------------#
# Enable this portion if the dates are already in file names and you just want a name corrections, 
# otherwise go to Part 2!!

## wd to the folder where your files are 

# setwd('./RawCSV_firstBatch/')
# setwd('./all_files_renamed/') #only enable this  if you did Part 3

## get current file names

#files <- list.files(pattern="*.csv$", full.names = T)

##get the dates

#dates_extracted <- regmatches(files,regexpr('\\d{4}-\\d{2}-\\d{2}',files))

## rename again

#file.rename(from = files, to = paste0(TYPE_NAME_HERE, "_", dates_extracted,"_",
#                                      FILE_CONTAINS,  "_", WHICH_DATASET,  "_", 
#                                     1:length(files), ".csv")) 

#----------------------------------------------------------- Part 2 -------------------------------------#
# Use this Part if you naming your file for the firt time and DON'T have date in the files name yet

##wd to the folder where your files are 

# setwd('./RawCSV_firstBatch/')
# setwd('./all_files_renamed/') #only enable this  if you did Part 3

##get current file names

#files <- list.files(pattern="*.csv$", full.names = T)

##rename the file with date {-----------------------> in linux, do file.info(files)$mtime <---------------}

#file.rename(from = files, to = paste0(TYPE_NAME_HERE, "_", format(file.mtime(files),"%Y-%m-%d"),"_",
#                                      FILE_CONTAINS,  "_", WHICH_DATASET, "_",
#                                      1:length(files), ".csv")) 


#----------------------------------------------------------- Part 3 -------------------------------------#
# Enable this Part if you have multiple folders, but files in them look same as the files in other folder

#for this part you will have to go back and forth to folder to folder then once you have the folder name in the
# file names, you will move all those file to central location, and run Part 1 or 2 based on need.

## walk in the main folder
setwd('./RawCSV_firstBatch/') #here root_directory/subdirectory (foler/nested folder)
dir.create("./all_files_renamed/") #make a directory in there

## walk in the nested folder
setwd('./New folder (1)/') #here root_directory/subdirectory (foler/nested folder)
nested_folder<-"./New folder (1)/"

files<-list.files( pattern="*.csv$")

# get the folder name
folder_name<- unlist(strsplit(nested_folder,"/"))[length(unlist(strsplit(nested_folder,"/")))]

file.rename(from = files,to = paste0(folder_name,"_", files))

#get the filenames with folder_name attached, move it to the central location
files<-list.files( pattern="*.csv$")

#go back twice move to central
file.copy(files,
          to = "../all_files_renamed/", recursive = T,
          overwrite = T, copy.mode = T, copy.date = F)

setwd('..') #goes back once!

#-----------#
#now go back to the next nested folder, !!!! REPEAT THIS STEP, I THINK YOU UNDERSTOOD WHAT WE'RE DOING !!!!
#-----------#

## walk in the nested folder
setwd('./New folder (2)/') #here root_directory/subdirectory (foler/nested folder)
nested_folder<-"./New folder (2)/"

files<-list.files( pattern="*.csv$")

# get the folder name
folder_name<- unlist(strsplit(nested_folder,"/"))[length(unlist(strsplit(nested_folder,"/")))]

file.rename(from = files,to = paste0(folder_name,"_", files))

#get the filenames with folder_name attached, move it to the central location
files<-list.files( pattern="*.csv$")

#go back twice move to central
file.copy(files,
          to = "../all_files_renamed/", recursive = T,
          overwrite = T, copy.mode = T, copy.date = F)

setwd('..') #goes back once!

# Now go to centeral location folder, rename everything there using Part 1 (filename has date) or 2 (fresh start, want date)!!

#EOF