#For USF SAIL
#~Mihir 

library(readr)
library(data.table)
library(dplyr)

#-------------------------------------------------- read botometer scores ---------------------------------------
# read the portion files
setwd('C:\\Users\\ThinkPad\\Desktop\\New folder (5)\\march_set_edLi\\portions')

#filenames 
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))
full_portions <- data.table::rbindlist(temp, fill = T) #make a dt
full_portions <- full_portions[!duplicated(full_portions$user_screen_name), ]

# read the scores files
setwd('C:\\Users\\ThinkPad\\Desktop\\New folder (5)\\march_set_edLi\\scores')

#filenames 
files <- list.files( pattern="*.csv$")

#read only columns that we want
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

#cut the dataframe
full_portions <- full_portions[ ,2:3]

setwd('C:\\Users\\ThinkPad\\Desktop\\New folder (5)')

#-------------------------------------------------- read the edglist file ---------------------------------------

edgelist_file <- data.table:::fread("csv_edgeList_march_ab6.csv")

rm(temp)
#-------------------------------------------------- attach the scores & target to original edgelist ------------

#source only df
so_df <- full_portions[full_portions$category=="Source", ]

#target only df
tar_df <- full_portions[full_portions$category=="Target", ]


#now trim the edgelist
edgelist_file <- edgelist_file[(edgelist_file$Source %in% so_df$user_screen_name), ]

# #then trim the edgelist
# edgelist_file <- edgelist_file[(edgelist_file$Target %in% tar_df$user_screen_name), ]

readr::write_excel_csv(edgelist_file, "march_edgelist_botometer_filter_mask_onSource.csv")

# #average the bot score fluctuation
# source_bot <- source_bot[ ,.(display_scores_english.Sou= weighted.mean(display_scores_english.Sou, na.rm=T),
#                              display_scores_universal.Sou=weighted.mean(display_scores_universal.Sou, na.rm=T), 
#                              cap_english.Sou=weighted.mean(cap_english.Sou,na.rm=T),
#                              cap_universal.Sou=weighted.mean(cap_universal.Sou,na.rm=T)),by= Source]
# 
# target_bot <- target_bot[ ,.(display_scores_english.Tar= weighted.mean(display_scores_english.Tar, na.rm=T),
#                              display_scores_universal.Tar=weighted.mean(display_scores_universal.Tar, na.rm=T), 
#                              cap_english.Tar=weighted.mean(cap_english.Tar,na.rm=T),
#                              cap_universal.Tar=weighted.mean(cap_universal.Tar,na.rm=T)),by= Target]
# 
# 
# # cut the source dt with >0.5 cap_english
# source_bot <- source_bot[source_bot$cap_english.Sou >0.5, ]
# 
# # cut the target dt with >0.5 cap_english
# target_bot <- target_bot[target_bot$cap_english.Tar >0.5, ]


#join, it automatically drops the NA
# edgelist_file <- target_bot[edgelist_file, on = .(Target), mult = "first", nomatch= 0L]
# 
# edgelist_file <- source_bot[edgelist_file, on = .(Source), mult = "first", nomatch= 0L]

# form a new edgelist

#first trim with SOURCE
#edgelist_file <- edgelist_file[(edgelist_file$Source %in% source_bot$Source), ]

#then trim with SOURCE
#edgelist_file <- edgelist_file[(edgelist_file$Target %in% target_bot$Target), ]


# readr::write_excel_csv(edgelist_file, "march_edgelist_botometer_filter.csv")

