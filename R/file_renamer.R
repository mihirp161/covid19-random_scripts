#For USF SAIL
# ~Mihir

# wd to the folder where your files are
setwd('./RawCSV_firstBatch/')

#------UPDATE THESE----------#
TYPE_NAME_HERE <- "mihir"
WHICH_DATASET <- "USC"
FILE_CONTAINS <- "btscr"
#---------------------------#

# get current file names
files <- list.files(pattern="*.csv$", full.names = T)

#----------------------------------------------------------- Part 1 -------------------------------------#
# Enable this portion if the dates are already in file names and you just want a name corrections, 
# otherwise go to Part 2!!

#get the dates
dates_extracted <- regmatches(files,regexpr('\\d{4}-\\d{2}-\\d{2}',files))

# rename again
file.rename(from = files, to = paste0(TYPE_NAME_HERE, "_", dates_extracted,"_",
                                      FILE_CONTAINS,  "_", WHICH_DATASET,  "_", 
                                      1:length(files), ".csv")) 

#----------------------------------------------------------- Part 2 -------------------------------------#
# Use this Part if you naming your file for the firt time and DON'T have date in the files name yet

#rename the file with date {-----------------------> in linux, do file.info(files)$mtime <---------------}
file.rename(from = files, to = paste0(TYPE_NAME_HERE, "_", format(file.mtime(files),"%Y-%m-%d"),"_",
                                      FILE_CONTAINS,  "_", WHICH_DATASET, "_",
                                      1:length(files), ".csv")) 

setwd('..')
#EOF