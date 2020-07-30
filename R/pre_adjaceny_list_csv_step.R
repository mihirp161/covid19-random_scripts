# For USF SAIL
# ~Mihir

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

#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/userr/march/')
setwd('./userr/')

#filenames
files <- list.files(pattern="*.csv$")

#read files
temp <- lapply(files, function(x) readr::read_csv(x))
user_data <- data.table::rbindlist(temp, fill = T) 

rm(temp)
setwd('..')

#-------- read tweet data

#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/march_usc/first_2_week_march_usc/')
setwd("./first_week_march/")

#filenames
files <- list.files( pattern="*.csv$")

#read files
temp <- lapply(files, function(x) readr::read_csv(x))
tweet_data <- data.table::rbindlist(temp, fill = T) 

rm(temp)
setwd('..')

#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/march_usc/last_2_week_march_usc/')
setwd('./last_week_march/')

#filenames
files <- list.files( pattern="*.csv$")

#read files
temp <- lapply(files, function(x) readr::read_csv(x))
tweet_data2 <- data.table::rbindlist(temp, fill = T) 

rm(temp)
setwd('..')

#-------- quoted tweet data

#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/quotedr/march/')
setwd('./quotedr/')

#filenames
files <- list.files( pattern="*.csv$")

#read files
temp <- lapply(files, function(x) readr::read_csv(x))
quoted_data <- data.table::rbindlist(temp, fill = T) 

rm(temp)
setwd('..')


#-------- reply data

#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/replyr/march/')
setwd('./replyr/')

#filenames
files <- list.files( pattern="*.csv$")

#read files
temp <- lapply(files, function(x) readr::read_csv(x))
reply_data <- data.table::rbindlist(temp, fill = T) 

rm(temp)
setwd('..')

#setwd('/shares_bgfs/si_twitter/Dred-MPColab/MIHIR PERSONAL TEMP/')

#---------- bind to make one big df
#merged <- cbind(tweet_data, user_data[,3:ncol(user_data)])

# first row bing two tweet data
ori_tweet_data <- dplyr::bind_rows(tweet_data, tweet_data2)

rm(tweet_data, tweet_data2)

# then tweet and reply
merged <- dplyr::inner_join(reply_data, ori_tweet_data, by = c("user.id_str", "id_str"), copy = FALSE)
#remove duplicates id_strs
merged <- merged[-which(duplicated(merged$id_str)), ] #remove the duplicates

rm(reply_data, ori_tweet_data)

# then tweet+reply and user
merged <- dplyr::inner_join(user_data, merged, by = c("user.id_str", "user.screen_name"), copy = FALSE)
#remove duplicates id_strs
merged <- merged[-which(duplicated(merged$id_str)), ]
rm(user_data)


# then tweet+reply+user and quoted
merged <- dplyr::inner_join(quoted_data, merged, by = c("user.id_str", "id_str"), copy = FALSE)
#remove duplicates id_strs
merged <- merged[-which(duplicated(merged$id_str)), ]
rm(quoted_data)


#add the comeplete text column 
merged$complete_texts <- ifelse(!grepl('^RT', merged$text),
                                            yes = ifelse((nchar(merged$text) < nchar(merged$extended_tweet.full_text))& !is.na(merged$extended_tweet.full_text),
                                                         yes = merged$extended_tweet.full_text,
                                                         no = merged$text
                                            ),
                                            no = ifelse((nchar(merged$retweeted_status.text) < nchar(merged$retweeted_status.extended_tweet.full_text))& !is.na(merged$retweeted_status.extended_tweet.full_text),
                                                        yes = merged$retweeted_status.extended_tweet.full_text,
                                                        no = merged$retweeted_status.text
                                            )
                                      )

#write to a query ready file
readr::write_excel_csv(merged, "first_july_2_weeks_dre.csv")
