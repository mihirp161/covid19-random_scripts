# For USF SAIL
# ~Dre, Mihir

options(scipen = 99999, warn = -1, stringsAsFactors = FALSE)
library(dplyr)
library(readr)
library(data.table)
#library(purrr)
#library(parallel)
# library(doParallel)
# library(snow)
#library(future)
#-------- read user data

# setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/march_usc/first_2_week_march_usf')
# 
# #filenames
# files <- list.files(pattern="*.csv$")
# 
# #read files
# temp <- lapply(files, function(x) readr::read_csv(x))
# user_data <- data.table::rbindlist(temp, fill = T) 
# 
# setwd('..')

#-------- read tweet data

setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/june_dres/first_2_week_june_dre')

#filenames
files <- list.files( pattern="*.csv$")

#read files
temp <- lapply(files, function(x) readr::read_csv(x))
tweet_data <- data.table::rbindlist(temp, fill = T) 

setwd('..')


#---------- bind to make one big df
#edge_list_read_csv <- cbind(tweet_data, user_data[,3:ncol(user_data)])

#merged <- inner_join(users, tweets, by = "user.id_str", copy = FALSE)
#merged <- merged[-which(duplicated(merged$id_str)),]

edge_list_read_csv <-tweet_data

#add the comeplete text column 
edge_list_read_csv$complete_texts <- ifelse(!grepl('^RT', edge_list_read_csv$text),
                                            yes = ifelse((nchar(edge_list_read_csv$text) < nchar(edge_list_read_csv$extended_tweet.full_text))& !is.na(edge_list_read_csv$extended_tweet.full_text),
                                                         yes = edge_list_read_csv$extended_tweet.full_text,
                                                         no = edge_list_read_csv$text
                                            ),
                                            no = ifelse((nchar(edge_list_read_csv$retweeted_status.text) < nchar(edge_list_read_csv$retweeted_status.extended_tweet.full_text))& !is.na(edge_list_read_csv$retweeted_status.extended_tweet.full_text),
                                                        yes = edge_list_read_csv$retweeted_status.extended_tweet.full_text,
                                                        no = edge_list_read_csv$retweeted_status.text
                                            )
                                      )

#write to a query ready file
readr::write_excel_csv(edge_list_read_csv, "first_june_2_weeks_dre.csv")
