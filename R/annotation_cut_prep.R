# SAIL Labs
# ~Mihir

## Description:
##-------------
#* This file was used to give twitter accounts to annotators based on certain 
#*  formula. This file needs a botometer scores as well as the human led annotations.
##-------------

library(data.table)
library(tidyverse)

#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('/files_path_here/')

#filenames
files <- list.files( pattern="*.csv$")

#read files
temp <- lapply(files, function(x) readr::read_csv(x))
data <- data.table::rbindlist(temp, fill = T) #make a df

setwd('..')

#----------------------
#formula (OLD)
# 1. use the most recent twitter accounts collected (perhaps between July 1-July 14)
# 2. using the botometer score, sample 500 accounts following the formula:
#   a. 40% from CAP >0.8
#   b. 40% from CAP <0.4
#   c. 20% from CAP between 0.4 and 0.8
# 3. create the spreadsheet pre-defined by Meagan (double-check with Meagan regarding the format of the spreadsheet.)


# all_8 <- data %>%
#           dplyr::filter(cap_english > 0.8)
# 
# all_5 <- data %>%
#           dplyr::filter(cap_english < 0.4)
# 
# all_in_4_and_8 <- data %>%
#                     dplyr::filter(cap_english >= 0.4 & cap_english <= 0.8)
# 
# #randomised the placements
# more_than_8 <- all_8[sample(.N, ceiling(0.40*nrow(all_8)), replace = F)]
# 
# less_than_4 <-  all_5[sample(.N, ceiling(0.40*nrow(all_5)), replace = F)]
# 
# between_4_and_8 <- all_in_4_and_8[sample(.N, ceiling(0.20*nrow(all_in_4_and_8)), replace = F)]
# 
# #shffle the rows randomly withou replacement
# set.seed(786)
# rbinded_dfs <- dplyr::bind_rows(more_than_8,less_than_4,between_4_and_8)

#----------------------
# Current formula (NEW)

# a. 50% from >= 0.8
# b. 50% from <0.8 

setwd('/file_path_here/')

data <- data.table::fread("read_annotatio_file.csv")

#----OR, use portion below if multiple files------------
#files <- list.files( pattern="*.csv$")

#read only columns that we want
# temp <- lapply(files, function(x) readr::read_csv(x))
# data <- data.table::rbindlist(temp, fill = T) #make a df

#setwd('..')

all_5.a <- data %>%
          dplyr::filter(cap_english >= 0.8)

all_5.b <- data %>%
          dplyr::filter(cap_english < 0.8)

#randomised the placements
above_8 <- all_5.a[sample(.N, ceiling(0.50*nrow(all_5.a)), replace = F)]

below_8 <-  all_5.b[sample(.N, ceiling(0.50*nrow(all_5.b)), replace = F)]

#shffle the rows randomly withou replacement
set.seed(786)

total_dfs <- dplyr::bind_rows(above_8, below_8)
  
rbinded_dfs <- dplyr::bind_rows(above_8[sample(.N, ceiling(0.50*60), replace = F)],
                                below_8[sample(.N, ceiling(0.50*60), replace = F)])


#add the urls for profile (some may get suspended or change their names)
rbinded_dfs$profile_url <- paste0("https://twitter.com/", rbinded_dfs$user_screen_name)


# shuffled_rows <- sample(nrow(rbinded_dfs), replace = F) #uncomment this when needed (OLD)
# rbinded_dfs <- rbinded_dfs[shuffled_rows, ]
# 
# sam_ndf <- rbinded_dfs[1:20,]
#
#sam_ndf <- sam_ndf %>% dplyr::select(user_screen_name, profile_url, cap_english)

# resize the dataframe
sam_ndf <- rbinded_dfs %>% dplyr::select(user_screen_name, profile_url, cap_english)

#rename columns
names(sam_ndf)[names(sam_ndf) == 'user_screen_name'] <- 'ScreenName'
names(sam_ndf)[names(sam_ndf) == 'profile_url'] <- 'URL'

#shuffle the row placements
shuffled_rows <- sample(nrow(sam_ndf), replace = F)
sam_ndf <- sam_ndf[shuffled_rows, ]

#make distributatble file
readr::write_excel_csv(sam_ndf,"sample_set_for_distribution.csv")

#subtract the people who have been picked be overwriting the file
readr::write_excel_csv(total_dfs,"full_file.csv")

#EOF