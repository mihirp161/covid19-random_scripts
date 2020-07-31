library(data.table)
library(tidyverse)

#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('C:\\Users\\ThinkPad\\Desktop\\July Botometer USF DATA 1\\july mihir botometer')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))
data <- data.table::rbindlist(temp, fill = T) #make a df

setwd('..')
#This work uses botometer files created between June 15-30 (Botometer ran on 07-03 to 07-05)

#----------------------
#formula
# 1. use the most recent twitter accounts collected (perhaps between July 1-July 14)
# 2. using the botometer score, sample 500 accounts following the formula:
#   a. 40% from CAP >0.8
#   b. 40% from CAP <0.4
#   c. 20% from CAP between 0.4 and 0.8
# 3. create the spreadsheet pre-defined by Meagan (double-check with Meagan regarding the format of the spreadsheet.)


# all_8 <- data %>%
#           dplyr::filter(cap_english > 0.8)
# 
# all_4 <- data %>%
#           dplyr::filter(cap_english < 0.4)
# 
# all_in_4_and_8 <- data %>%
#                     dplyr::filter(cap_english >= 0.4 & cap_english <= 0.8)
# 
# #randomised the placements
# more_than_8 <- all_8[sample(.N, ceiling(0.40*nrow(all_8)), replace = F)]
# 
# less_than_4 <-  all_4[sample(.N, ceiling(0.40*nrow(all_4)), replace = F)]
# 
# between_4_and_8 <- all_in_4_and_8[sample(.N, ceiling(0.20*nrow(all_in_4_and_8)), replace = F)]
# 
# #shffle the rows randomly withou replacement
# set.seed(786)
# rbinded_dfs <- dplyr::bind_rows(more_than_8,less_than_4,between_4_and_8)

#----------------------
# new formula

# a. 60% from >= 0.8
# b. 40% from <0.8 

setwd('C:\\Users\\ThinkPad\\Desktop\\July Botometer USF DATA 1')

data <- data.table::fread("july_people_for_annotation_10P_run3_special.csv")

# OR------------
#files <- list.files( pattern="*.csv$")

#read only columns that we want
# temp <- lapply(files, function(x) readr::read_csv(x))
# data <- data.table::rbindlist(temp, fill = T) #make a df

#setwd('..')

all_6 <- data %>%
          dplyr::filter(cap_english >= 0.8)

all_4 <- data %>%
          dplyr::filter(cap_english < 0.8)

#randomised the placements
above_8 <- all_6[sample(.N, ceiling(0.50*nrow(all_6)), replace = F)]

below_8 <-  all_4[sample(.N, ceiling(0.50*nrow(all_4)), replace = F)]

#shffle the rows randomly withou replacement
set.seed(786)

total_dfs <- dplyr::bind_rows(above_8, below_8)
  
rbinded_dfs <- dplyr::bind_rows(above_8[sample(.N, ceiling(0.50*60), replace = F)],
                                below_8[sample(.N, ceiling(0.50*60), replace = F)])


#add the urls for profile (some may get suspended or change their names)
rbinded_dfs$profile_url <- paste0("https://twitter.com/", rbinded_dfs$user_screen_name)


# shuffled_rows <- sample(nrow(rbinded_dfs), replace = F)
# rbinded_dfs <- rbinded_dfs[shuffled_rows, ]
# 
# sam_ndf <- rbinded_dfs[1:20,]

#sam_ndf <- sam_ndf %>% dplyr::select(user_screen_name, profile_url, cap_english)

sam_ndf <- rbinded_dfs %>% dplyr::select(user_screen_name, profile_url, cap_english)


names(sam_ndf)[names(sam_ndf) == 'user_screen_name'] <- 'ScreenName'
names(sam_ndf)[names(sam_ndf) == 'profile_url'] <- 'URL'

shuffled_rows <- sample(nrow(sam_ndf), replace = F)
sam_ndf <- sam_ndf[shuffled_rows, ]

#subtract the people who have been picked

readr::write_excel_csv(sam_ndf,"july_people_for_annotation_10P_run3_special.csv")

readr::write_excel_csv(total_dfs,"july_people_full_run3.csv")

