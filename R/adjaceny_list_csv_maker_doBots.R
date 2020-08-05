#For USF SAIL
#~Mihir 

library(readr)
library(data.table)
#library(igraph)
#library(rgexf)
library(dplyr)

#-------------------------------------------------- read botometer scores ---------------------------------------
# read the target files
setwd('./BOT_CSV_USC_March_Target/')

#filenames 
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))
target_bot <- data.table::rbindlist(temp, fill = T) #make a dt

colnames(target_bot) <- c("Target", "display_scores_english.Tar", "display_scores_universal.Tar",
                          "cap_english.Tar", "cap_universal.Tar")
setwd('..')

# read the source files
setwd('./BOT_CSV_USC_March_Source/')

#filenames 
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))
source_bot <- data.table::rbindlist(temp, fill = T) #make a dt

colnames(source_bot) <- c("Source", "display_scores_english.Sou", "display_scores_universal.Sou",
                          "cap_english.Sou", "cap_universal.Sou")

setwd('..')
#-------------------------------------------------- read the edglist file ---------------------------------------

edgelist_file <- data.table:::fread("edgeList_march_ab6_3k.csv")

x <- data.table:::fread("edgeList_march_ab6_3k.csv")

rm(temp)
#-------------------------------------------------- attach the scores & target to original edgelist ------------


#average the bot score fluctuation
source_bot <- source_bot[ ,.(display_scores_english.Sou= weighted.mean(display_scores_english.Sou, na.rm=T),
                             display_scores_universal.Sou=weighted.mean(display_scores_universal.Sou, na.rm=T), 
                             cap_english.Sou=weighted.mean(cap_english.Sou,na.rm=T),
                             cap_universal.Sou=weighted.mean(cap_universal.Sou,na.rm=T)),by= Source]

target_bot <- target_bot[ ,.(display_scores_english.Tar= weighted.mean(display_scores_english.Tar, na.rm=T),
                             display_scores_universal.Tar=weighted.mean(display_scores_universal.Tar, na.rm=T), 
                             cap_english.Tar=weighted.mean(cap_english.Tar,na.rm=T),
                             cap_universal.Tar=weighted.mean(cap_universal.Tar,na.rm=T)),by= Target]


# cut the source dt with >0.5 cap_english
source_bot <- source_bot[source_bot$cap_english.Sou >0.5, ]

# cut the target dt with >0.5 cap_english
target_bot <- target_bot[target_bot$cap_english.Tar >0.5, ]


#join, it automatically drops the NA
# edgelist_file <- target_bot[edgelist_file, on = .(Target), mult = "first", nomatch= 0L]
# 
# edgelist_file <- source_bot[edgelist_file, on = .(Source), mult = "first", nomatch= 0L]

# form a new edgelist

#first trim with SOURCE
edgelist_file <- edgelist_file[(edgelist_file$Source %in% source_bot$Source), ]

#then trim with SOURCE
#edgelist_file <- edgelist_file[(edgelist_file$Target %in% target_bot$Target), ]


readr::write_excel_csv(edgelist_file, "march_edgelist_botometer_filter.csv")

#-------------------------------------------------------------------------------------------------------------------
edgelist_file_ori <- data.table:::fread("csv_edgeList_march_ab6.csv")

# edgelist_file[!duplicated(edgelist_file$Source), ]







