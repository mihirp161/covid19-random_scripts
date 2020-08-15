# SAIL Labs
# ~Mihir

## Description:
##-------------
#* Use this script if we need to filter the edgelist csv (the file that we generate)
#* from adjaceny_list_csv_maker.R file. In order to run this, you will need botometer
#* scores.
##-------------

library(readr)
library(data.table)
library(dplyr)

#-------------------------------------------------- read botometer scores ---------------------------------------
# read the portion files
setwd('/path_to_files/')

#filenames 
files <- list.files( pattern="*.csv$")

#read the files
temp <- lapply(files, function(x) readr::read_csv(x))
full_portions <- data.table::rbindlist(temp, fill = T) #make a dt
full_portions <- full_portions[!duplicated(full_portions$user_screen_name), ]

# read the bot scores files
setwd('/path_to_files/')

#filenames 
files <- list.files( pattern="*.csv$")

#read the files
temp <- lapply(files, function(x) readr::read_csv(x))
scores <- data.table::rbindlist(temp, fill = T) #make a dt

# average the bot score fluctuation if repeated users are present due to cycle
scores <- scores[ ,.(display_scores_english= weighted.mean(display_scores_english, na.rm=T),
                     display_scores_universal=weighted.mean(display_scores_universal, na.rm=T),
                     cap_english=weighted.mean(cap_english,na.rm=T),
                     cap_universal=weighted.mean(cap_universal,na.rm=T)),by= user_screen_name]

# cut the scores dt with >0.5 cap_english
scores <- scores[scores$cap_english >0.5, ]

#first trim with full_proportion
full_portions <- full_portions[(full_portions$user_screen_name %in% scores$user_screen_name), ]

#cut the dataframe with only keeping 2nd and 3rd columns
full_portions <- full_portions[ ,2:3]

setwd('/path_to_files/')

#-------------------------------------------------- read the edglist file ---------------------------------------

#read the normal edgelist csv file
edgelist_file <- data.table:::fread("csv_edgeList_file.csv")

rm(temp) #remove temp variable
#-------------------------------------------------- attach the scores & target to original edgelist ------------

#extract source only df
so_df <- full_portions[full_portions$category=="Source", ]

#extract target only df
tar_df <- full_portions[full_portions$category=="Target", ]

#now trim the edgelist
edgelist_file <- edgelist_file[(edgelist_file$Source %in% so_df$user_screen_name), ]

# then trim the edgelist (only when asked)
# edgelist_file <- edgelist_file[(edgelist_file$Target %in% tar_df$user_screen_name), ]

# this file be filter with botometer scores now
readr::write_excel_csv(edgelist_file, "filtered_edgelist_file.csv")

# EOF
