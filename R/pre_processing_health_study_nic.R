# SAIL Labs
# ~Mihir

## Description:
#* This script does some pre-processing for Dr Nic's data for Health agencies' social engagement study.


options(scipen = 9999)

library(tidyverse)
library(readxl)
library(rio)
library(lubridate)
library(data.table)

# rollback the directory
go_back <- function(){
  setwd("..")
}

# counts the duplicates based on columns
count_dups <- function(tbl, column){
  return(tbl %>% janitor::get_dupes(which(colnames(tbl)==column)) %>% dplyr::tally())
}


# returns a dataframe with all files combined
tbl_maker <- function(main_dir, sub_dir, which_files, go_back_this_many, date_col_name, remove_unknown= TRUE){
  
  setwd(paste0("./", main_dir)) # set the directories
  setwd(paste0("./", sub_dir))

  files <- list.files(pattern = "*.csv$", recursive = TRUE) #get all csvs
  files <- files[grepl(which_files, files)] #get filenames
  tbl <- files %>% purrr::map_df(~read_csv(., col_types = cols(.default = "c"))) #merge files
  if(remove_unknown) {
    tbl <- tbl[-1] #remove UNKNOWN column
  }
  
  tbl[ ,date_col_name] <- as.POSIXct(tbl[[date_col_name]],
                                          format="%Y-%m-%d %H:%M:%S", tz = "GMT") #re-classify datatype to dates, twitter is in GMT

  lapply(seq_len(go_back_this_many), function(x) go_back()) #run function n times
  
  return(tbl)
}

# removes duplicates from the dataframe
remove_dups <- function(tbl, column){
  
  tbl <- tbl[-which(duplicated(tbl[ ,column])), ] #keep only unique items
  tbl <- tbl[!is.na(tbl[ ,column]), ] #keep only the populated cells
  
  return(tbl)
}


# EST time for facebook
date1_EST <- as.POSIXct("2020-01-01 00:00:00 EST") #lower bound
date2_EST <- as.POSIXct("2021-01-01 00:00:00 EST") #upper bound

rng_EST <- lubridate::interval(date1_EST, date2_EST) #desired range

# GMT time for twitter
date1_GMT <- as.POSIXct("2020-01-01 00:00:00 GMT") #lower bound
date2_GMT <- as.POSIXct("2021-01-01 00:00:00 GMT") #upper bound

rng_GMT <- lubridate::interval(date1_GMT, date2_GMT) #desired range


#-------------------------------------------------- Step 1 -----------------------------------

# Read CSV files at many levels
## TWITTER FILES

# state level

state_tbl <- dplyr::bind_rows(tbl_maker(main_dir= "original-selected", 
                                        sub_dir= "state_health_tweets_2020_12_07",
                                        which_files= "tweet",
                                        go_back_this_many= 2,
                                        date_col_name= "created_at"), 
                              tbl_maker(main_dir= "original-selected", 
                                        sub_dir= "state_health_tweets_2021_01_07",
                                        which_files= "tweet",
                                        go_back_this_many= 2,
                                        date_col_name= "created_at"))


count_dups(state_tbl, "status_id")

state_tbl <- remove_dups(state_tbl, "status_id")

count_dups(state_tbl, "status_id")

state_tbl <- state_tbl[state_tbl$created_at %within% rng_GMT, ]


readr::write_csv(state_tbl, "state_health_tweets_2020.csv")


# local level


local_tbl <- dplyr::bind_rows(tbl_maker(main_dir= "original-selected", 
                                        sub_dir= "local_health_tweets_2020_12_14",
                                        which_files= "tweet",
                                        go_back_this_many= 2,
                                        date_col_name= "created_at"), 
                              tbl_maker(main_dir= "original-selected", 
                                        sub_dir= "local_health_tweets_2021_01_07",
                                        which_files= "tweet",
                                        go_back_this_many= 2,
                                        date_col_name= "created_at"))

count_dups(local_tbl, "status_id")

local_tbl <- remove_dups(local_tbl, "status_id")

count_dups(local_tbl, "status_id")

local_tbl <- local_tbl[local_tbl$created_at %within% rng_GMT, ]

readr::write_csv(local_tbl, "local_health_tweets_2020.csv")


# Federal level

fed_tbl <- dplyr::bind_rows(tbl_maker(main_dir= "original-selected", 
                                        sub_dir= "fed_health_tweets_2020-2021",
                                        which_files= "tweet",
                                        go_back_this_many= 2,
                                        date_col_name= "created_at"))

count_dups(fed_tbl, "status_id")

fed_tbl <- remove_dups(fed_tbl, "status_id")

count_dups(fed_tbl, "status_id")

fed_tbl <-  fed_tbl[fed_tbl$created_at %within% rng_GMT, ]


readr::write_csv(fed_tbl, "federal_health_tweets_2020.csv")


## FACEBOOK

setwd(paste0("./", "original-selected"))

# read the excel file, and move sheets into specific dataframes
data_list <- rio::import_list("Health_Related_Governmental_FB_Pages_withFollowerCount.xlsx")

state_tbl_fb <- data_list$`State Level`

local_tbl_fb <-  data_list$`Local Level`

fed_tbl_fb <- data_list$`Federal Level`

setwd('..')

# Filter Facebbook time

# Facebook times are in eastern times
state_tbl_fb$Created <- as.POSIXct(state_tbl_fb$Created, format="%Y-%m-%d %H:%M:%S", tz = "America/New_York") 

fed_tbl_fb$Created <- as.POSIXct(fed_tbl_fb$Created, format="%Y-%m-%d %H:%M:%S", tz = "America/New_York") 

local_tbl_fb$Created <- as.POSIXct(local_tbl_fb$Created, format="%Y-%m-%d %H:%M:%S", tz = "America/New_York") 


state_tbl_fb <- state_tbl_fb[state_tbl_fb$Created %within% rng_EST, ]

local_tbl_fb <- local_tbl_fb[local_tbl_fb$Created %within% rng_EST, ]

fed_tbl_fb <- fed_tbl_fb[fed_tbl_fb$Created %within% rng_EST, ]


#create facebook files

readr::write_csv(state_tbl_fb, "state_health_fb_2020.csv")
readr::write_csv(fed_tbl_fb, "federal_health_fb_2020.csv")
readr::write_csv(local_tbl_fb, "local_health_fb_2020.csv")

#------------------------------------------------- Step 2 ----------------------------------------

# emotional egagement in facebook

# sum positive and negative reactions and attach them
attach_eng <- function(tbl){
  
  pos_cols <- c('Likes','Love','Care','Wow','Haha')
  neg_cols <- c('Sad','Angry')
  
  tbl$positive_eng <- apply(tbl[ ,pos_cols],1,sum)
  tbl$negative_eng <- apply(tbl[ ,neg_cols],1,sum)
  
  return(tbl)
}

# create engagement dataframes
state_tbl_fb_eng <- attach_eng(state_tbl_fb) %>%
                      dplyr::select(`User Name`, Created,
                                    positive_eng, negative_eng) %>%
                      dplyr::mutate_at(vars(Created), funs(as.Date))

local_tbl_fb_eng <- attach_eng(local_tbl_fb)%>%
                      dplyr::select(`User Name`, Created,
                                    positive_eng, negative_eng) %>%
                      dplyr::mutate_at(vars(Created), funs(as.Date))

fed_tbl_fb_eng <- attach_eng(fed_tbl_fb)%>%
                      dplyr::select(`User Name`, Created,
                                    positive_eng, negative_eng) %>%
                      dplyr::mutate_at(vars(Created), funs(as.Date))

# get engagement per day
state_tbl_fb_eng <- state_tbl_fb_eng %>%
                      dplyr::group_by(`User Name`, Created) %>%
                      dplyr::summarise_each(funs(sum))

local_tbl_fb_eng <- local_tbl_fb_eng %>%
                      dplyr::group_by(`User Name`, Created) %>%
                      dplyr::summarise_each(funs(sum))

fed_tbl_fb_eng <- fed_tbl_fb_eng %>%
                      dplyr::group_by(`User Name`, Created) %>%
                      dplyr::summarise_each(funs(sum))


readr::write_csv(state_tbl_fb_eng, "state_health_fb_eng.csv")
readr::write_csv(fed_tbl_fb_eng, "federal_health_fb_eng.csv")
readr::write_csv(local_tbl_fb_eng, "local_health_fb_eng.csv")

#------------------------------------------------- Step 3 ----------------------------------------

# Fix time difference

## TWITTER

#state health agencies
#local health agencies
setwd(paste0("./", "original-selected"))

# read the health agencies file to get their time zones hours

state_time_file <- readr::read_csv("state_health_agencies.csv") %>% 
                      dplyr::mutate(agency_twitter_handle= tolower(agency_twitter_handle),
                                    agency_fb_handle = tolower(agency_fb_handle))

local_time_file <- readr::read_csv("local_health_agencies.csv") %>% 
                      dplyr::mutate(agency_Twitter_handle= tolower(agency_Twitter_handle),
                                    agency_FB_handle = tolower(agency_FB_handle))
  
setwd('..')

#join state to state_tbl and local to local_tbl

state_tbl <- state_tbl %>% dplyr::mutate(screen_name = tolower(screen_name)) %>%
                dplyr::left_join(state_time_file, by = c("screen_name" = "agency_twitter_handle"))

local_tbl <- local_tbl %>% dplyr::mutate(screen_name = tolower(screen_name)) %>%
                dplyr::left_join(local_time_file, by = c("screen_name" = "agency_Twitter_handle"))


# Check which elements didn't match
setdiff(factor(state_time_file$agency_twitter_handle), factor(state_tbl$screen_name))
setdiff(factor(local_time_file$agency_Twitter_handle), factor(local_tbl$screen_name))

# remove the rows with missing time difference
remove_rows_missing_on_column <- function(tbl, column){
  all_vector <- complete.cases(tbl[, column])
  return(tbl[all_vector, ])
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ AS PER FEB 26 DIRECTION, NO NEED TO ADJUST TIMES ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# # fix federal agencies
# fed_tbl$corrected_time <-  fed_tbl$created_at - lubridate::hours(x= 5)
# 
# 
# # clear the missing rows
# state_tbl <- remove_rows_missing_on_column(state_tbl, c("gmt_difference"))
# local_tbl <- remove_rows_missing_on_column(local_tbl, c("gmt_difference"))
# 
# 
# # turn the values to positive vals
# state_tbl$gmt_difference <- abs(state_tbl$gmt_difference)
# local_tbl$gmt_difference <- abs(local_tbl$gmt_difference)
# 
# # attach the fixed times to designated tables
# state_tbl$corrected_time <- state_tbl$created_at - lubridate::hours(x= state_tbl$gmt_difference)
# 
# local_tbl$corrected_time <- local_tbl$created_at - lubridate::hours(x= local_tbl$gmt_difference)
# 
# # write to the csvs
# readr::write_csv(state_tbl, "state_tweets_joined_timed.csv")
# readr::write_csv(fed_tbl, "federal_health_tweets_timed.csv")
# readr::write_csv(local_tbl, "local_tweets_joined_timed.csv")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# aggregation measures (for Twitter)

state_tbl_cut <- state_tbl %>%
                    dplyr::select(created_at, screen_name, name, favorite_count, retweet_count, agency_name,
                                         state, state_code, gov_party, state_pop_2010) %>%
                    dplyr::mutate_at(vars(created_at), funs(as.Date))


local_tbl_cut <- local_tbl %>%
                    dplyr::select(created_at, screen_name, name, favorite_count, retweet_count, agency_name,
                                  state, city_gov_party_2020, city_pop, most_pop_city, county, state_gov_party)%>%
                    dplyr::mutate_at(vars(created_at), funs(as.Date))


fed_tbl_cut <- fed_tbl %>%
                    dplyr::select(created_at, screen_name, name, favorite_count, retweet_count)%>%
                    dplyr::mutate_at(vars(created_at), funs(as.Date))

# federal level
fed_tbl_complete <- fed_tbl_cut %>%
                      dplyr::group_by(created_at, screen_name, name) %>%
                      dplyr::summarise(daily_favorite_count = toString(favorite_count),
                                       daily_retweet_count = toString(retweet_count)) %>%
                      dplyr::ungroup() %>% 
                      tidyr::separate_rows(daily_favorite_count,daily_retweet_count , convert = TRUE) %>% 
                      dplyr::group_by(created_at, screen_name, name) %>% 
                      dplyr::summarise(total_daily_posts= dplyr::n_distinct(daily_favorite_count),
                                daily_favorite_count = sum(daily_favorite_count),
                                daily_retweet_count = sum(daily_retweet_count))

# local level
local_tbl_complete <- local_tbl_cut %>%
                      dplyr::group_by(created_at, screen_name, name, agency_name,
                                      state, county, state_gov_party, most_pop_city, city_gov_party_2020, city_pop) %>%
                      dplyr::summarise(daily_favorite_count = toString(favorite_count),
                                       daily_retweet_count = toString(retweet_count)) %>%
                      dplyr::ungroup() %>% 
                      tidyr::separate_rows(daily_favorite_count,daily_retweet_count , convert = TRUE) %>% 
                      dplyr::group_by(created_at, screen_name, name, agency_name,
                                      state,county, state_gov_party, most_pop_city, city_gov_party_2020, city_pop) %>% 
                      dplyr::summarise(total_daily_posts= dplyr::n_distinct(daily_favorite_count),
                                       daily_favorite_count = sum(daily_favorite_count),
                                       daily_retweet_count = sum(daily_retweet_count))

# state level
state_tbl_complete <- state_tbl_cut %>%
                         dplyr::group_by(created_at, screen_name, name, agency_name,
                                         state, state_code, gov_party, state_pop_2010) %>%
                         dplyr::summarise(daily_favorite_count = toString(favorite_count),
                                          daily_retweet_count = toString(retweet_count)) %>%
                         dplyr::ungroup() %>% 
                         tidyr::separate_rows(daily_favorite_count,daily_retweet_count , convert = TRUE) %>% 
                         dplyr::group_by(created_at, screen_name, name, agency_name,
                                         state, state_code, gov_party, state_pop_2010) %>% 
                         dplyr::summarise(total_daily_posts= dplyr::n_distinct(daily_favorite_count),
                                          daily_favorite_count = sum(daily_favorite_count),
                                          daily_retweet_count = sum(daily_retweet_count))

#create new files
readr::write_csv(state_tbl_complete, "state_tweets_daily_complete.csv")
readr::write_csv(fed_tbl_complete, "federal_tweets_daily_complete.csv")
readr::write_csv(local_tbl_complete, "local_tweets_daily_complete.csv")

#------------------------------------------------- Step 4 ----------------------------------------

# Fix the time difference (For FACEBOOK)

## FACEBOOK

# duplicate federal, local, and state file
fed_tbl_fb_eng_timed <- dplyr::as_tibble(fed_tbl_fb)
state_tbl_fb_eng_timed <- dplyr::as_tibble(state_tbl_fb)
local_tbl_fb_eng_timed <- dplyr::as_tibble(local_tbl_fb)

#join state to state_tbl and local to local_tbl

state_tbl_fb_eng_timed <- state_tbl_fb_eng_timed %>% dplyr::mutate(`User Name` = tolower(`User Name`)) %>%
                            dplyr::left_join(state_time_file, by = c("User Name" = "agency_fb_handle"))

local_tbl_fb_eng_timed <- local_tbl_fb_eng_timed %>% dplyr::mutate(`User Name` = tolower(`User Name`)) %>% 
                            dplyr::left_join(local_time_file, by = c("User Name" = "agency_FB_handle"))

# Check which elements don't match
setdiff(factor(state_time_file$agency_fb_handle), factor(state_tbl_fb_eng_timed$`User Name`))
setdiff(factor(local_time_file$agency_FB_handle), factor(local_tbl_fb_eng_timed$`User Name`))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ AS PER FEB 26 DIRECTION, NO NEED TO ADJUST TIMES ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# # fix the federal hours
# fed_tbl_fb_eng_timed$corrected_time <- fed_tbl_fb_eng_timed$Created - lubridate::hours(x= 5)
# 
# # clear the missing rows
# state_tbl_fb_eng_timed <- remove_rows_missing_on_column(state_tbl_fb_eng_timed, c("gmt_difference"))
# local_tbl_fb_eng_timed <- remove_rows_missing_on_column(local_tbl_fb_eng_timed, c("gmt_difference"))
# 
# 
# # turn the values to positive vals
# state_tbl_fb_eng_timed$gmt_difference <- abs(state_tbl_fb_eng_timed$gmt_difference)
# local_tbl_fb_eng_timed$gmt_difference <- abs(local_tbl_fb_eng_timed$gmt_difference)
# 
# # attach the fixed times to designated tables
# state_tbl_fb_eng_timed$corrected_time <- state_tbl_fb_eng_timed$Created - lubridate::hours(x= state_tbl_fb_eng_timed$gmt_difference)
# 
# local_tbl_fb_eng_timed$corrected_time <- local_tbl_fb_eng_timed$Created - lubridate::hours(x= local_tbl_fb_eng_timed$gmt_difference)
# 
# # write to the csvs
# readr::write_csv(state_tbl_fb_eng_timed, "state_fb_joined_timed.csv")
# readr::write_csv(fed_tbl_fb_eng_timed, "federal_fb_timed.csv")
# readr::write_csv(local_tbl_fb_eng_timed, "local_fb_joined_timed.csv")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# aggregation measures (for Facebook

state_tbl_fb_cut <- state_tbl_fb_eng_timed %>%
                      dplyr::select(Created, `User Name`, `Page Name`, 
                                    Likes, Love, Care, Wow, Haha,
                                    Sad, Angry, 
                                    `Total Interactions`, Comments, Shares, 
                                    state, state_code, gov_party, state_pop_2010) %>%
                      dplyr::mutate_at(vars(Created), funs(as.Date))


local_tbl_fb_cut <- local_tbl_fb_eng_timed %>%
                    dplyr::select(Created, `User Name`, `Page Name`, 
                                  Likes, Love, Care, Wow, Haha,
                                  Sad, Angry, 
                                  `Total Interactions`, Comments, Shares, 
                                   city_gov_party_2020,
                                  city_pop, level, most_pop_city, county, state_gov_party)%>%
                    dplyr::mutate_at(vars(Created), funs(as.Date))


fed_tbl_fb_cut <- fed_tbl_fb_eng_timed %>%
                    dplyr::select(Created, `User Name`, `Page Name`, 
                                  Likes, Love, Care, Wow, Haha,
                                  Sad, Angry, 
                                  `Total Interactions`, Comments, Shares)%>%
                    dplyr::mutate_at(vars(Created), funs(as.Date))

# federal level
fed_tbl_complete_fb <- fed_tbl_fb_cut %>%
                          attach_eng() %>%
                          dplyr::group_by(Created, `User Name`, `Page Name`) %>%
                          dplyr::summarise(daily_positive_eng = toString(positive_eng),
                                           daily_negative_eng = toString(negative_eng),
                                           daily_total_interactions = toString(`Total Interactions`),
                                           daily_comments = toString(Comments),
                                           daily_shares = toString(Shares)) %>%
                          dplyr::ungroup() %>% 
                          tidyr::separate_rows(daily_positive_eng, daily_negative_eng, daily_total_interactions,
                                               daily_comments, daily_shares, convert = TRUE) %>% 
                          dplyr::group_by(Created, `User Name`, `Page Name`) %>% 
                          dplyr::summarise(total_daily_posts= dplyr::n_distinct(daily_shares),
                                           daily_positive_eng = sum(daily_positive_eng),
                                           daily_negative_eng = sum(daily_negative_eng),
                                           daily_total_interactions = sum(daily_total_interactions),
                                           daily_comments = sum(daily_comments),
                                           daily_shares = sum(daily_shares))


# local level
local_tbl_complete_fb <- local_tbl_fb_cut %>%
                        attach_eng() %>%
                        dplyr::group_by(Created, `User Name`, `Page Name`, 
                                        most_pop_city, county, state_gov_party, city_gov_party_2020,
                                        city_pop, level) %>%
                        dplyr::summarise(daily_positive_eng = toString(positive_eng),
                                         daily_negative_eng = toString(negative_eng),
                                         daily_total_interactions = toString(`Total Interactions`),
                                         daily_comments = toString(Comments),
                                         daily_shares = toString(Shares)) %>%
                        dplyr::ungroup() %>% 
                        tidyr::separate_rows(daily_positive_eng, daily_negative_eng, daily_total_interactions,
                                             daily_comments, daily_shares, convert = TRUE) %>% 
                        dplyr::group_by(Created, `User Name`, `Page Name`, 
                                        most_pop_city, county, state_gov_party, city_gov_party_2020,
                                        city_pop, level) %>% 
                        dplyr::summarise(total_daily_posts= dplyr::n_distinct(daily_shares),
                                         daily_positive_eng = sum(daily_positive_eng),
                                         daily_negative_eng = sum(daily_negative_eng),
                                         daily_total_interactions = sum(daily_total_interactions),
                                         daily_comments = sum(daily_comments),
                                         daily_shares = sum(daily_shares))

# state level
state_tbl_complete_fb <- state_tbl_fb_cut %>%
                          attach_eng() %>%
                          dplyr::group_by(Created, `User Name`, `Page Name`, 
                                          state, state_code, gov_party, state_pop_2010) %>%
                          dplyr::summarise(daily_positive_eng = toString(positive_eng),
                                           daily_negative_eng = toString(negative_eng),
                                           daily_total_interactions = toString(`Total Interactions`),
                                           daily_comments = toString(Comments),
                                           daily_shares = toString(Shares)) %>%
                          dplyr::ungroup() %>% 
                          tidyr::separate_rows(daily_positive_eng, daily_negative_eng, daily_total_interactions,
                                               daily_comments, daily_shares, convert = TRUE) %>% 
                          dplyr::group_by(Created, `User Name`, `Page Name`, 
                                          state, state_code, gov_party, state_pop_2010) %>% 
                          dplyr::summarise(total_daily_posts= dplyr::n_distinct(daily_shares),
                                           daily_positive_eng = sum(daily_positive_eng),
                                           daily_negative_eng = sum(daily_negative_eng),
                                           daily_total_interactions = sum(daily_total_interactions),
                                           daily_comments = sum(daily_comments),
                                           daily_shares = sum(daily_shares))

#create new files
readr::write_csv(state_tbl_complete_fb, "state_fb_daily_complete.csv")
readr::write_csv(fed_tbl_complete_fb, "federal_fb_daily_complete.csv")
readr::write_csv(local_tbl_complete_fb, "local_fb_daily_complete.csv")

#------------------------------------------------------- Step 5 ---------------------------------------

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ AS PER FEB 26 DIRECTION, NO NEED TO USE JHU DATA STATE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# setwd(paste0("./", "original-selected"))
# setwd(paste0("./", "JHU_covid_data_state"))
# 
# filenames <- list.files(pattern="*.csv", full.names=TRUE)
# 
# 
# setwd("..")
# setwd("..")


# integrate state covid incidence data into one file
jhu_covid_data_state <- tbl_maker(main_dir= "original-selected", 
                                  sub_dir= "JHU_covid_data_state",
                                  which_files= "2020",
                                  go_back_this_many= 2,
                                  date_col_name= "Last_Update",
                                  remove_unknown = FALSE)


jhu_covid_data_state <- jhu_covid_data_state[tolower(jhu_covid_data_state$Province_State) != "recovered", ]

jhu_covid_data_state_cut <- jhu_covid_data_state %>%
                              dplyr::select(Last_Update, Province_State, Confirmed, Deaths)%>%
                              dplyr::mutate_at(vars(Last_Update), funs(as.Date))

DATE1 <- as.Date("2020-01-01")
DATE2 <- as.Date("2020-12-31")

jhu_covid_data_state_cut <- jhu_covid_data_state_cut%>%
                              dplyr::filter(Last_Update >= DATE1 & Last_Update <= DATE2)


jhu_covid_data_state_51 <- jhu_covid_data_state_cut %>%
                             dplyr::group_by(Last_Update, Province_State) %>%
                             dplyr::summarise(total_confirmed = toString(Confirmed),
                                              total_deaths = toString(Deaths)) %>%
                             dplyr::ungroup()%>%
                             tidyr::separate_rows(total_confirmed, total_deaths, convert = TRUE)
  
readr::write_csv(jhu_covid_data_state_51, "state_covid_incidence_2020.csv")



# create federal covid incidence file
jhu_covid_data_fed_51 <- jhu_covid_data_state_cut %>%
                          dplyr::select(-Province_State) %>%
                          dplyr::group_by(Last_Update) %>%
                          dplyr::summarise(total_confirmed = toString(Confirmed),
                                           total_deaths = toString(Deaths)) %>%
                          dplyr::ungroup()%>%
                          tidyr::separate_rows(total_confirmed, total_deaths, convert = TRUE) %>%
                          dplyr::group_by(Last_Update) %>%
                          dplyr::summarise(total_confirmed= sum(total_confirmed),
                                           total_deaths = sum(total_deaths))

readr::write_csv(jhu_covid_data_fed_51, "federal_covid_incidence_2020.csv")


# Local incidence 

# read all the necessary data files
setwd(paste0("./", "original-selected"))

counties <- readr::read_csv("counties_of_interest.csv")

setwd("..")

setwd(paste0("./", "original-selected"))
setwd(paste0("./", "JHU_covid_data_county"))

jhu_confirmed <- readr::read_csv("time_series_covid19_confirmed_US.csv")
jhu_death <- readr::read_csv("time_series_covid19_deaths_US.csv")

setwd("..")

setwd(paste0("./", "JHU_covid_data_state"))

filenames <- list.files(pattern="*.csv", full.names=TRUE)
jhu_data_state <- lapply(filenames, readr::read_csv)




setwd("..")
setwd("..")
# swap the columns in dataframe

counties <- counties[ ,c(2,1)]

# create a column marking counties values
counties['marker'] <- 1

# join the two, keeping all of JHU's indices
jhu_confirmed_filtered <- dplyr::left_join(jhu_confirmed, counties, 
                                           by= c("Admin2" = "county", "Province_State" = "state")) %>%
                          tidyr::drop_na(marker) %>%
                          dplyr::select(-marker)     

jhu_death_filtered <- dplyr::left_join(jhu_death, counties, 
                                           by= c("Admin2" = "county", "Province_State" = "state")) %>%
                          tidyr::drop_na(marker) %>%
                          dplyr::select(-marker)                


# spread the data so all the dates become rows and rest become columns
library(reshape2)

# here we are changing the names to something proper
jhu_death_filtered <- dplyr::rename(jhu_death_filtered,
                          c("county"= "Admin2", "state" = "Province_State"))

jhu_confirmed_filtered <-  dplyr::rename(jhu_confirmed_filtered,
                                         c("county"= "Admin2", "state" = "Province_State"))

# melt death df
jhu_death_filtered_inci <- reshape2::melt(jhu_death_filtered %>%
                           dplyr::select(
                             -"UID",-"iso2",-"iso3",-"code3",-"FIPS",
                             -"Country_Region",-"Lat",-"Long_",-"Combined_Key",-"Population"
                           ), id.vars=c("county","state"))%>%
                          dplyr::rename("date"="variable" , "deaths"= "value" ) %>%
                          dplyr::group_by(county, state, date) %>%
                          dplyr::summarise(deaths= sum(deaths))

# melt confirmed df
jhu_confirmed_filtered_inci <- reshape2::melt(jhu_confirmed_filtered %>%
                                            dplyr::select(
                                              -"UID",-"iso2",-"iso3",-"code3",-"FIPS",
                                              -"Country_Region",-"Lat",-"Long_",-"Combined_Key"
                                            ), id.vars=c("county","state"))%>%
                              dplyr::rename("date"="variable" , "confirmed"= "value" ) %>%
                              dplyr::group_by(county, state, date) %>%
                              dplyr::summarise(confirmed= sum(confirmed))

# join the deaths and confirmed table
combined_local_inci <- dplyr::left_join(jhu_confirmed_filtered_inci, jhu_death_filtered_inci,
                                        by= c("county", "state", "date"))

#change the date column to POSIXCT
combined_local_inci$date <- lubridate::mdy(combined_local_inci$date)

# cut the dataframe to 2020 only
combined_local_inci <- combined_local_inci%>%
                        dplyr::filter(date >= DATE1 & date <= DATE2)

#write to file
readr::write_csv(combined_local_inci, "local_covid_incidence_2020.csv")


#-------------------------------------- Step 6 --------------------------------------

# join the state complete tweets with state incidence
state_covid_2020 <- dplyr::inner_join(state_tbl_complete, jhu_covid_data_state_51, by= c("corrected_time" = "Last_Update",
                                                                                         "state" = "Province_State"))

readr::write_csv(state_covid_2020, "state_tweets_covid_2020.csv")

# join facebook complete file with state incidence
state_covid_2020_fb <- dplyr::inner_join(state_tbl_complete_fb, jhu_covid_data_state_51, by= c("corrected_time" = "Last_Update",
                                                                                         "state" = "Province_State"))

readr::write_csv(state_covid_2020_fb, "state_fb_covid_2020.csv")

# join the federal fields

fed_covid_2020 <- dplyr::inner_join(fed_tbl_complete, jhu_covid_data_fed_51, by= c("corrected_time" = "Last_Update"))

readr::write_csv(fed_covid_2020, "federal_tweets_covid_2020.csv")

fed_covid_2020_fb <- dplyr::inner_join(fed_tbl_complete_fb, jhu_covid_data_fed_51, by= c("corrected_time" = "Last_Update"))

readr::write_csv(fed_covid_2020_fb, "federal_fb_covid_2020.csv")


# compress the local incidence file
compressed_local <- combined_local_inci %>%
                    dplyr::group_by(state, date) %>%
                    dplyr::summarize(county = stringr::str_c(county, collapse = ", "),
                                  confirmed= sum(confirmed),
                                  deaths= sum(deaths))

# check which rows have counties seperated by commas so you can compress them to a single county
View(compressed_local %>%
      dplyr::filter_all(dplyr::any_vars(grepl(',', .))))

# reduced all New York counties to New york only
compressed_local$county <- ifelse(grepl(',', compressed_local$county), "New York",  compressed_local$county)

readr::write_csv(compressed_local, "local_covid_incidence_2020_corrected.csv")

# check which rows have counties seperated by commas so you can compress them to a single county

local_covid_2020 <- dplyr::inner_join(local_tbl_complete, compressed_local, by= c("corrected_time" = "date",
                                                                                    "county" = "county")) %>%
                    dplyr::select(-state.y) %>%
                    dplyr::rename("state" = "state.x")
            
readr::write_csv(local_covid_2020, "local_tweets_covid_2020.csv")

# finally make a local facebook couty file

local_covid_2020_fb <- dplyr::inner_join(local_tbl_complete_fb, compressed_local, by= c("corrected_time" = "date",
                                                                                  "county" = "county"))

readr::write_csv(local_covid_2020_fb, "local_fb_covid_2020.csv")

#---------------------------------------- Step 7 -----------------------------------------

# Creating state tweets/fb and covid file for early 2020
setwd(paste0("./", "original-selected"))
setwd(paste0("./", "JHU_covid_data_county"))

ts_cov19_confirmed <- readr::read_csv("time_series_covid19_confirmed_US.csv") %>% 
                        dplyr::rename(c("county"= "Admin2", "state" = "Province_State"))

# melt all confirmed and death cases and group them by cases and date and sum them
ts_cov19_confirmed <- reshape2::melt(ts_cov19_confirmed %>%
                             dplyr::select(
                               -"UID",-"iso2",-"iso3",-"code3",-"FIPS",
                               -"Country_Region",-"Lat",-"Long_",-"Combined_Key"
                             ), id.vars=c("county","state"))%>%
                      dplyr::rename("date"="variable" , "confirmed"= "value" ) %>%
                      dplyr::mutate(date= lubridate::mdy(date)) %>%
                      dplyr::group_by(state, date) %>%
                      dplyr::summarise(confirmed= sum(confirmed))

ts_cov19_deaths <- readr::read_csv("time_series_covid19_deaths_US.csv") %>% 
                      dplyr::rename(c("county"= "Admin2", "state" = "Province_State"))

ts_cov19_deaths <- reshape2::melt(ts_cov19_deaths %>%
                                    dplyr::select(
                                      -"UID",-"iso2",-"iso3",-"code3",-"FIPS",
                                      -"Country_Region",-"Lat",-"Long_",-"Combined_Key",-"Population"
                                    ), id.vars=c("county","state"))%>%
                    dplyr::rename("date"="variable" , "deaths"= "value" ) %>%
                    dplyr::mutate(date= lubridate::mdy(date)) %>%
                    dplyr::group_by(state, date) %>%
                    dplyr::summarise(deaths= sum(deaths))

DATE1 <- as.Date("2020-01-22")
DATE2 <- as.Date("2020-05-01")

ts_cov19_confirmed <- ts_cov19_confirmed %>%
                        #dplyr::mutate(date= lubridate::mdy(date)) %>%
                        dplyr::filter(date >= DATE1 & date <= DATE2)


ts_cov19_deaths <- ts_cov19_deaths %>%
                        #dplyr::mutate(date= lubridate::mdy(date)) %>%
                        dplyr::filter(date >= DATE1 & date <= DATE2)

ts_state_2020 <- dplyr::inner_join(ts_cov19_confirmed, ts_cov19_deaths, by= c("state" = "state",
                                                                              "date" = "date"))


setwd("..")
setwd("..")

readr::write_csv(ts_state_2020, "state_covid_incidence_early2020_test.csv")


#-------------------------------------Compare the JHU figures -------------------------------------

# full join the JHU dataset to JHU county dataset for record purpose
DATE1 <- as.Date("2020-04-12")
DATE2 <- as.Date("2020-05-01")  
  
tsts_state_2020_CUT <- ts_state_2020 %>%
                        dplyr::filter(date >= DATE1 & date <= DATE2)%>%
                        dplyr::rename("confirmed_JHU_covid_data_county" = "confirmed",
                                      "deaths_JHU_covid_data_county" = "deaths")


jhu_covid_data_state_51_CUT <- jhu_covid_data_state_51 %>%
                                  dplyr::filter(Last_Update >= DATE1 & Last_Update <= DATE2) %>%
                                  dplyr::rename("confirmed_JHU_covid_data_state" = "total_confirmed",
                                                "deaths_JHU_covid_data_state" = "total_deaths")

x <- dplyr::left_join(tsts_state_2020_CUT, jhu_covid_data_state_51_CUT, by= c("state" = "Province_State",
                                                                              "date" = "Last_Update"))

readr::write_csv(x, "comparing_JHU_figures_4_12to5_01.csv")

#--------------------------------------------Green font --------------------------------------------

# now keep only the matches rows between state incidence and county from Jan 22 to May 1

DATE1 <- as.Date("2020-01-22")
DATE2 <- as.Date("2020-05-01")

tsts_state_2020_jan22_may1 <- ts_state_2020 %>%
                                  dplyr::filter(date >= DATE1 & date <= DATE2)

jhu_covid_data_state_51_jan22_may1  <- jhu_covid_data_state_51 %>%
                                          dplyr::filter(Last_Update >= DATE1 & Last_Update <= DATE2)


unmatched_entries_state <- dplyr::anti_join(tsts_state_2020_jan22_may1, jhu_covid_data_state_51_jan22_may1,
                                            by= c("state" = "Province_State",
                                                  "date" = "Last_Update",
                                                  "confirmed" = "total_confirmed",
                                                  "deaths" = "total_deaths"))


readr::write_csv(unmatched_entries_state, "state_covid_incidence_early2020.csv")


#------------------------------------------------Sky Blue font ---------------------------------

# join county data from Jan 22 to May 1 with daily tweets

state_tbl_complete_51_jan22_may1 <- state_tbl_complete

# state_tbl_complete_51_jan22_may1 <- state_tbl_complete %>%
#                                           dplyr::filter(corrected_time >= DATE1 & corrected_time <= DATE2)

matched_entries_state <- dplyr::inner_join(unmatched_entries_state, state_tbl_complete_51_jan22_may1,
                                                by= c("state" = "state",
                                                      "date" = "corrected_time"))


readr::write_csv(matched_entries_state, "state_tweets_covid_early2020.csv")

#-------------------------------------------------Orange font ---------------------------------

# join county data from Jan 22 to May 1 with daily facebook post

state_tbl_complete_51_jan22_may1_fb <- state_tbl_complete_fb

# state_tbl_complete_51_jan22_may1_fb <- state_tbl_complete_fb %>%
#                                       dplyr::filter(corrected_time >= DATE1 & corrected_time <= DATE2)

matched_entries_state_fb <- dplyr::inner_join(unmatched_entries_state, state_tbl_complete_51_jan22_may1_fb,
                                              by= c("state" = "state",
                                                    "date" = "corrected_time"))

readr::write_csv(matched_entries_state_fb, "state_fb_covid_early2020.csv")

#------------------------------------------------- purple font -----------------------------------------

# Join state_tweets_covid_early2020 with state_tweets_covid_2020

matched_entries_state <- matched_entries_state %>%
                            dplyr::rename("total_confirmed" = "confirmed",
                                          "total_deaths" = "deaths",
                                          "corrected_time" = "date") %>%
                            dplyr::select(corrected_time, screen_name, name, agency_name,
                                          state, state_code, gov_party, state_pop_2010, total_daily_posts,
                                          daily_favorite_count, daily_retweet_count, total_confirmed, 
                                          total_deaths)

state_tweets_covid_full2020 <- dplyr::bind_rows(matched_entries_state, state_covid_2020) %>% 
                                  dplyr::distinct(corrected_time, state,  .keep_all = T)

readr::write_csv(state_tweets_covid_full2020, "state_tweets_covid_full2020.csv")

#--------------------------------------------- Red font -------------------------------------------

# Join state_tweets_covid_early2020 with state_fb_covid_2020

matched_entries_state_fb <- matched_entries_state_fb %>%
                              dplyr::rename("total_confirmed" = "confirmed",
                                            "total_deaths" = "deaths",
                                            "corrected_time" = "date") %>%
                              dplyr::select(corrected_time, `User Name`, `Page Name`, state,
                                        state_code, gov_party, state_pop_2010, total_daily_posts,
                                        daily_positive_eng, daily_negative_eng, daily_total_interactions, 
                                        daily_comments, daily_shares, total_confirmed, total_deaths)


state_tweets_covid_full2020_fb<- dplyr::bind_rows(matched_entries_state_fb, state_covid_2020_fb) %>% 
                                    dplyr::distinct(corrected_time, state,  .keep_all = T)

readr::write_csv(state_tweets_covid_full2020_fb, "state_fb_covid_full2020.csv")


#---------------------------------------------------Green font OK OK OK ----------------------------------

# reapeating the sames steps. We can create functions too, but I just want to finish this. Sigh.

# Creating federal tweets/fb and covid file for early 2020

ts_state_2020 <- ts_state_2020 %>%
                    dplyr::select(-state) %>%
                    dplyr::group_by(date) %>%
                    dplyr::summarise(deaths = sum(deaths), 
                                     confirmed = sum(confirmed))

# now keep only the matches rows between federal incidence and county from Jan 22 to May 1

DATE1 <- as.Date("2020-01-22")
DATE2 <- as.Date("2020-05-01")

tsts_state_2020_jan22_may1_fed <- ts_state_2020 %>%
                                dplyr::filter(date >= DATE1 & date <= DATE2)

jhu_covid_data_state_51_jan22_may1_fed  <- jhu_covid_data_fed_51 %>%
                                            dplyr::filter(Last_Update >= DATE1 & Last_Update <= DATE2)


unmatched_entries_state_fed <- dplyr::anti_join(tsts_state_2020_jan22_may1_fed, jhu_covid_data_state_51_jan22_may1_fed,
                                            by= c("date" = "Last_Update",
                                                  "confirmed" = "total_confirmed",
                                                  "deaths" = "total_deaths"))


readr::write_csv(unmatched_entries_state_fed, "federal_covid_incidence_early2020.csv")

#---------------------------------------------------Sky Blue font OK OK OK -----------------------------------

# join county data from Jan 22 to May 1 with daily tweets

state_tbl_complete_51_jan22_may1_fed <- fed_tbl_complete

# state_tbl_complete_51_jan22_may1_fed <- fed_tbl_complete %>%
#                                           dplyr::filter(corrected_time >= DATE1 & corrected_time <= DATE2)

matched_entries_state_fed <- dplyr::inner_join(unmatched_entries_state_fed, state_tbl_complete_51_jan22_may1_fed,
                                           by= c("date" = "corrected_time"))


readr::write_csv(matched_entries_state_fed, "federal_tweets_covid_early2020.csv")


#-------------------------------------------------Orange font OK OK OK --------------------------------------

# join county data from Jan 22 to May 1 with daily facebook post

state_tbl_complete_51_jan22_may1_fb_fed <- fed_tbl_complete_fb

# state_tbl_complete_51_jan22_may1_fb_fed <- fed_tbl_complete_fb %>%
#                                               dplyr::filter(corrected_time >= DATE1 & corrected_time <= DATE2)

matched_entries_state_fb_fed <- dplyr::inner_join(unmatched_entries_state_fed, state_tbl_complete_51_jan22_may1_fb_fed,
                                              by= c("date" = "corrected_time"))

readr::write_csv(matched_entries_state_fb_fed, "federal_fb_covid_early2020.csv")


#------------------------------------------------- purple font OK OK OK -------------------------------------

# Join state_tweets_covid_early2020 with federal tweets

matched_entries_state_fed <- matched_entries_state_fed %>%
                            dplyr::rename("total_confirmed" = "confirmed",
                                          "total_deaths" = "deaths",
                                          "corrected_time" = "date") %>%
                            dplyr::select(corrected_time, screen_name, name, total_daily_posts,
                                          daily_favorite_count, daily_retweet_count, total_confirmed,
                                          total_deaths)

state_tweets_covid_full2020_fed <- dplyr::bind_rows(matched_entries_state_fed, fed_covid_2020) %>% 
                                      dplyr::distinct(corrected_time, screen_name, .keep_all = T)

readr::write_csv(state_tweets_covid_full2020_fed, "federal_tweets_covid_full2020.csv")

#--------------------------------------------- Red font OK OK OK -------------------------------------------

# Join state_tweets_covid_early2020 with state_fb_covid_2020

matched_entries_state_fb_fed <- matched_entries_state_fb_fed %>%
                                    dplyr::rename("total_confirmed" = "confirmed",
                                                  "total_deaths" = "deaths",
                                                  "corrected_time" = "date") %>%
                                    dplyr::select(corrected_time, `User Name`, `Page Name`, total_daily_posts,
                                                  daily_positive_eng, daily_negative_eng, daily_total_interactions, 
                                                  daily_comments, daily_shares, total_confirmed, total_deaths)


state_tweets_covid_full2020_fb_fed <- dplyr::bind_rows(matched_entries_state_fb_fed, fed_covid_2020_fb) %>% 
                                    dplyr::distinct(corrected_time, `User Name`,  .keep_all = T)

readr::write_csv(state_tweets_covid_full2020_fb_fed, "federal_fb_covid_full2020.csv")
