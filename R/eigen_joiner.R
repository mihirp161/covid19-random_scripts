# SAIL Labs
# ~Mihir

## Description:
##-------------
#* joins the bot scores to edgelist csv files (OLD)
##-------------

library(tidyverse)
library(data.table)
library(plyr)

#read the original gephi csv output file
hunK_bot_analysisFile <- readr::read_csv("edgelist_file.csv")

#go to where expoerted csv files are (these are probably the one you get when you run populate_bot_fils.py)
setwd('./files_path/')

#filenames
files <- list.files( pattern="*.csv$")

#read only all columns
temp <- lapply(files, function(x) data.table::fread(x))
exports_data <- data.table::rbindlist(temp, fill = T) #make a df

setwd('..') #move the working directory back, or give an specific path

#renames the columns
colnames(exports_data) <- c("Id",  "display_scores_english" , "display_scores_universal", "cap_english", "cap_universal")

#join the files
complete_hunK_bot_file <- plyr::join(hunK_bot_analysisFile, exports_data, type="left")

#uncomment these if you were asked to not include NAs
#complete_hunK_bot_file <- merge(hunK_bot_analysisFile, exports_data, by="Id")

#write
readr::write_csv(complete_hunK_bot_file, "edgelist_file_with_bots.csv")

#EOF
