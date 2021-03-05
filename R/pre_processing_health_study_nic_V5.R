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
library(reshape2)

# rollback the directory
go_back <- function(){
  setwd("..")
}

# counts the duplicates based on columns
count_dups <- function(tbl, column){
  return(tbl %>% janitor::get_dupes(which(colnames(tbl)==column)) %>% dplyr::tally())
}


# returns a dataframe with all files combined
tbl_maker <- function(main_dir, sub_dir, which_files, go_back_this_many, date_col_name, is_retweet_col_name, remove_unknown= TRUE){
  
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
  
  # remove the rows that are retweeted
  tbl <- tbl[!(tbl[[is_retweet_col_name]] == TRUE), ]
  
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
                                        date_col_name= "created_at",
                                        is_retweet_col_name = "is_retweet"), 
                              tbl_maker(main_dir= "original-selected", 
                                        sub_dir= "state_health_tweets_2021_01_07",
                                        which_files= "tweet",
                                        go_back_this_many= 2,
                                        date_col_name= "created_at",
                                        is_retweet_col_name = "is_retweet"))


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
                                        date_col_name= "created_at",
                                        is_retweet_col_name = "is_retweet"), 
                              tbl_maker(main_dir= "original-selected", 
                                        sub_dir= "local_health_tweets_2021_01_07",
                                        which_files= "tweet",
                                        go_back_this_many= 2,
                                        date_col_name= "created_at",
                                        is_retweet_col_name = "is_retweet"))

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
                                      date_col_name= "created_at",
                                      is_retweet_col_name = "is_retweet"))

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

state_tbl <- state_tbl %>% 
              dplyr::mutate(screen_name = tolower(screen_name)) %>%
              dplyr::left_join(state_time_file, by = c("screen_name" = "agency_twitter_handle"))

local_tbl <- local_tbl %>% 
             dplyr::mutate(screen_name = tolower(screen_name)) %>%
             dplyr::left_join(local_time_file, by = c("screen_name" = "agency_Twitter_handle"))


# Check which elements didn't match
setdiff(factor(state_time_file$agency_twitter_handle), factor(state_tbl$screen_name))
setdiff(factor(local_time_file$agency_Twitter_handle), factor(local_tbl$screen_name))

# remove the rows with missing time difference
remove_rows_missing_on_column <- function(tbl, column){
  all_vector <- complete.cases(tbl[, column])
  return(tbl[all_vector, ])
}

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
                      dplyr::summarise(total_daily_posts= dplyr::n(),
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
                        dplyr::summarise(total_daily_posts= dplyr::n(),
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
                        dplyr::summarise(total_daily_posts= dplyr::n(),
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

state_tbl_fb_eng_timed <- state_tbl_fb_eng_timed %>%
                              dplyr::mutate(`User Name` = tolower(`User Name`)) %>%
                              dplyr::left_join(state_time_file, by = c("User Name" = "agency_fb_handle"))

local_tbl_fb_eng_timed <- local_tbl_fb_eng_timed %>%
                            dplyr::mutate(`User Name` = tolower(`User Name`)) %>% 
                            dplyr::left_join(local_time_file, by = c("User Name" = "agency_FB_handle"))

# Check which elements don't match
setdiff(factor(state_time_file$agency_fb_handle), factor(state_tbl_fb_eng_timed$`User Name`))
setdiff(factor(local_time_file$agency_FB_handle), factor(local_tbl_fb_eng_timed$`User Name`))


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
                          dplyr::summarise(total_daily_posts= dplyr::n(),
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
                            dplyr::summarise(total_daily_posts= dplyr::n(),
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
                            dplyr::summarise(total_daily_posts= dplyr::n(),
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

## here we are going to fix county files for confirmed and deaths csv files

# read both confirmed and death case files
setwd(paste0("./", "original-selected"))
setwd(paste0("./", "JHU_covid_data_county"))

jhu_confirmed <- readr::read_csv("time_series_covid19_confirmed_US.csv")
jhu_death <- readr::read_csv("time_series_covid19_deaths_US.csv")

setwd("..")
setwd("..")

jhu_confirmed <- dplyr::rename(jhu_confirmed,
                                c("county"= "Admin2", "state" = "Province_State"))

jhu_death <- dplyr::rename(jhu_death,
                            c("county"= "Admin2", "state" = "Province_State"))
#-------------------- melt confirm df

jhu_confirmed_newDaily <- reshape2::melt(jhu_confirmed,
                                         id.vars=c("county","state",
                                                   "UID","iso2","iso3","code3","FIPS",
                                                   "Country_Region","Lat","Long_","Combined_Key"))%>%
                          dplyr::rename("date"="variable" , "confirmed"= "value" ) 


jhu_confirmed_newDaily <- jhu_confirmed_newDaily %>%
                            dplyr::group_by(county, state,
                                            UID, iso2, iso3, code3, FIPS ,
                                            Country_Region, Lat , Long_, Combined_Key) %>%
                            dplyr::arrange(date) %>%
                            dplyr::mutate(confirmed = confirmed - dplyr::lag(confirmed, 
                                                                             default = dplyr::first(confirmed)))


# unmelt the previous dataframe
csv_jhu_confirmed_newDaily <- reshape2::dcast(jhu_confirmed_newDaily, ... ~ date, value.var = 'confirmed')

# remove vars we don't need
jhu_confirmed_newDaily <- jhu_confirmed_newDaily %>%
                           dplyr::ungroup() %>%
                           dplyr::select(
                             -c("UID","iso2","iso3","code3","FIPS",
                                "Country_Region","Lat","Long_","Combined_Key"))

readr::write_csv(csv_jhu_confirmed_newDaily, "covid19_confirmed_US_newDaily.csv")

#-------------------- melt death df

jhu_deaths_newDaily <- reshape2::melt(jhu_death,
                                         id.vars=c("county","state",
                                                   "UID", "iso2", "iso3", "code3", "FIPS",
                                                   "Country_Region", "Lat", "Long_", "Combined_Key", "Population"))%>%
                                    dplyr::rename("date"="variable" , "deaths"= "value" )


jhu_deaths_newDaily <- jhu_deaths_newDaily %>%
                            dplyr::group_by(county, state,
                                            UID, iso2, iso3, code3, FIPS ,
                                            Country_Region, Lat , Long_, Combined_Key, Population) %>%
                            dplyr::arrange(date) %>%
                            dplyr::mutate(deaths = deaths - dplyr::lag(deaths, 
                                                                        default = dplyr::first(deaths)))


# unmelt the previous dataframe
csv_jhu_death_newDaily <- reshape2::dcast(jhu_deaths_newDaily, ... ~ date, value.var = 'deaths')

# remove vars we don't need
jhu_deaths_newDaily <- jhu_deaths_newDaily %>%
                            dplyr::ungroup() %>%
                            dplyr::select(
                              -c("UID","iso2","iso3","code3","FIPS",
                                 "Country_Region","Lat","Long_","Combined_Key", "Population"))

readr::write_csv(csv_jhu_death_newDaily, "covid19_deaths_US_newDaily.csv")


#--------------- create incidence files

DATE1 <- as.Date("2020-01-01")
DATE2 <- as.Date("2020-12-31")

jhu_deaths_newDaily_ranged <- jhu_deaths_newDaily %>%
                              dplyr::mutate_at(vars(date), funs(lubridate::mdy)) %>%
                              dplyr::filter(date >= DATE1 & date <= DATE2)

jhu_confirmed_newDaily_ranged <- jhu_confirmed_newDaily %>%
                                dplyr::mutate_at(vars(date), funs(lubridate::mdy)) %>%
                                dplyr::filter(date >= DATE1 & date <= DATE2)

# State incidence
jhu_incidence <- dplyr::inner_join(jhu_confirmed_newDaily_ranged, jhu_deaths_newDaily_ranged, 
                                    by= c("county" = "county", "state" = "state", "date"="date")) %>%
                          tidyr::drop_na()

readr::write_csv(jhu_incidence, "incidence_2020.csv")

jhu_incidence_state <- jhu_incidence %>% 
                        dplyr::select(-county) %>%
                        dplyr::group_by(date, state) %>%
                        dplyr::summarise(total_confirmed = sum(confirmed),
                                         total_deaths = sum(deaths))


readr::write_csv(jhu_incidence_state, "state_covid_incidence_2020.csv")

# Federal incidence

jhu_incidence_fed <- jhu_incidence %>% 
                        dplyr::select(-c(county,state)) %>%
                        dplyr::group_by(date) %>%
                        dplyr::summarise(total_confirmed = sum(confirmed),
                                         total_deaths = sum(deaths))

readr::write_csv(jhu_incidence_fed, "federal_covid_incidence_2020.csv")


# local incidence

# read all the necessary data files
setwd(paste0("./", "original-selected"))

counties <- readr::read_csv("counties_of_interest.csv")

setwd("..")

counties <- counties[ ,c(2,1)]

# create a column marking counties values
counties['marker'] <- 1

# join the two, keeping all of JHU's indices
jhu_incidence_local <- dplyr::left_join(jhu_incidence, counties, 
                              by= c("county" = "county", "state" = "state")) %>%
                              tidyr::drop_na(marker) %>%
                              dplyr::select(-marker)%>%
                              dplyr::group_by(county, state, date) %>%
                              dplyr::summarise(total_confirmed= sum(confirmed),
                                               total_deaths = sum(deaths))     

readr::write_csv(jhu_incidence_local, "local_covid_incidence_2020.csv")


#--------------------------------------------- Step 6 ---------------------------------------

# join the state complete tweets with state incidence
state_covid_2020 <- dplyr::inner_join(state_tbl_complete, jhu_incidence_state, by= c("created_at" = "date",
                                                                                     "state" = "state"))

readr::write_csv(state_covid_2020, "state_tweets_covid_2020.csv")

# join facebook complete file with state incidence
state_covid_2020_fb <- dplyr::inner_join(state_tbl_complete_fb, jhu_incidence_state, by= c("Created" = "date",
                                                                                           "state" = "state"))

readr::write_csv(state_covid_2020_fb, "state_fb_covid_2020.csv")

# join the federal complete tweets with federal incidence

fed_covid_2020 <- dplyr::inner_join(fed_tbl_complete, jhu_incidence_fed, by= c("created_at" = "date"))

readr::write_csv(fed_covid_2020, "federal_tweets_covid_2020.csv")


# join facebook complete file with federal incidence
fed_covid_2020_fb <- dplyr::inner_join(fed_tbl_complete_fb, jhu_incidence_fed, by= c("Created" = "date"))

readr::write_csv(fed_covid_2020_fb, "federal_fb_covid_2020.csv")

# join local tweets files

# compress the local incidence file
jhu_incidence_local_compressed <- jhu_incidence_local %>%
                                    dplyr::group_by(state, date) %>%
                                    dplyr::summarize(county = stringr::str_c(county, collapse = ", "),
                                                     total_confirmed = sum(total_confirmed),
                                                     total_deaths = sum(total_deaths))

# check which rows have counties seperated by commas so you can compress them to a single county
View(jhu_incidence_local_compressed %>%
       dplyr::filter_all(dplyr::any_vars(grepl(',', .))))

# reduced all New York counties to New york only
jhu_incidence_local_compressed$county <- ifelse(grepl(',', jhu_incidence_local_compressed$county), 
                                                yes = "New York",  no = jhu_incidence_local_compressed$county)

readr::write_csv(jhu_incidence_local_compressed, "local_covid_incidence_2020_corrected.csv")

# now we can join local complete tweets with local incidence
local_covid_2020 <- dplyr::inner_join(local_tbl_complete, jhu_incidence_local_compressed, 
                                          by= c("created_at" = "date",
                                                 "county" = "county")) %>%
                      dplyr::select(-state.y) %>%
                      dplyr::rename("state" = "state.x")

readr::write_csv(local_covid_2020, "local_tweets_covid_2020.csv")


# finally join a local facebook file incidence local file

local_covid_2020_fb <- dplyr::inner_join(local_tbl_complete_fb, jhu_incidence_local_compressed,
                                            by= c("Created" = "date",
                                                  "county" = "county"))

readr::write_csv(local_covid_2020_fb, "local_fb_covid_2020.csv")
