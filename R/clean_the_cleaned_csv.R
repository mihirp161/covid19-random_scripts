# SAIL Labs
# ~Dre, Mihir

## Description:
##-------------
#* Takes a csv with 100s of columns and merged some of the columns if they share column names. 
#* Currently this script is using "+" as a delimiter however you can change it to whatever you like. 
#* Don't pick "," because they will end up creating columns.
##-------------


library(data.table)
library(tidyverse)

#Caution: Looks ugly, but it's better to add the delimitor in rather than nothing. So + seperate stuff. If NA then 
#               there would be no gap
#---------------------------------------------------- distinct columns stuff --------------------------------------

#singly columns, put additional ones in the furture
distinct_cols <- c("node_user_id", 
                   "node_user_screen_name", 
                   "node_user_created_at",
                   "node_user_location", 
                   "node_user_description", 
                   "node_user_followers_count",
                   "node_user_friends_count", 
                   "node_user_statuses_count", 
                   "node_timestamp_ms", 
                   "node_text", 
                   "node_id",
                   "node_retweeted_status_retweet_count",
                   "node_retweeted_status_extended_tweet_full_text",
                   "node_retweeted_status_user_screen_name",
                   "node_retweeted_status_user_id",
                   "node_retweeted_status_user_followers_count",
                   "node_retweeted_status_user_favourites_count",
                   "node_geo_coordinates_0",
                   "node_geo_coordinates_1",
                   "node_retweeted_status_geo_coordinates_0",
                   "node_retweeted_status_geo_coordinates_1")

# read the file here

distinct_dat <- as.data.frame(data.table::fread("2020-04-05-20_cleaned_cleaned.csv",
                                                  select = distinct_cols))
#initially we only read with columns we know distinctly exist
#we can use data table for this

#---------------------------------------------------- multiple columns stuff --------------------------------------

# Then here, & because we only care about the column names, we will read only the first row of the file
multi_cols_names <- colnames(read.csv("2020-04-05-20_cleaned_cleaned.csv",stringsAsFactors = F, nrows=1))

# we want to get these names at the moment:
#--- *** represents the numbers of many colums, if you do decide to add some in the future, please expand this list!
#1. nm1. "node_entities_user_mentions_***_screen_name",
#1. nm2. "node_entities_user_mentions_***_name"

#2. nm3. "node_retweeted_status_entities_urls_***_url",
#2. nm4. "node_retweeted_status_entities_urls_***_expanded_url,
#2. nm5.  "node_retweeted_status_entities_urls_***_display_url"

# we are going to extract every single repeated columns which we are intrested in
# I advise just have multiple greps, which is more secure than simple looping through a vector or something

#get all mentioned screen names and names columns. All node start same, end with same....name
matches_1 <- grep("^node_entities_user_mentions_.*_name$", multi_cols_names, ignore.case = T)

#read all the damn urls coulumns, notice all the starting and ending of the nodes. All nodes start same, end with same....url
matches_2 <- grep("^node_retweeted_status_entities_urls_.*_url$", multi_cols_names, ignore.case = T)

#fill in the vector to coulms we are inrested in
multi_cols <- c(multi_cols_names[matches_1], multi_cols_names[matches_2])

#now do the fread again to read all these columns
multiples_dat <- as.data.frame(data.table::fread("2020-04-05-20_cleaned_cleaned.csv",
                                                  select = multi_cols))


#----------------------------------- now we are going to make new columns deliminiated by ;---------------------------

#removes the columns that have been merged
remove_combined_cols <- function(nm_itr, df){
  for (co in nm_itr)
    df[co] <- NULL
  return(df)
}

#read all the screen_names FROM THE MULTIPLE DATAFRAME! Also don't rename now, do it later.
nm <- grep("node_entities_user_mentions.+screen_name$", colnames(multiples_dat), value = T)

#now merge all the columns with same name. I.e. text.1, text.2, text.3 and so on. Delimeter is a "+" symbol
multiples_dat$combined_node_entitied_screen_names<-  apply(multiples_dat[ , nm] , 1 , paste , collapse = "+" )

multiples_dat <- remove_combined_cols(nm, multiples_dat) #call the func. to remove the columns that we combined

#read all the names
nm <- grep("node_entities_user_mentions.+_name$", colnames(multiples_dat), value = T)

multiples_dat$combined_node_entitied_names<-  apply(multiples_dat[ , nm] , 1 , paste , collapse = "+" )

multiples_dat <- remove_combined_cols(nm, multiples_dat)

#read all the display_url
nm <- grep("node_retweeted_status_entities_urls.+_display_url$", colnames(multiples_dat), value = T)

multiples_dat$combined_node_entitied_names<-  apply(multiples_dat[ , nm] , 1 , paste , collapse = "+" )

multiples_dat <- remove_combined_cols(nm, multiples_dat)

#read all the display_url
nm <- grep("node_retweeted_status_entities_urls.+_display_url$", colnames(multiples_dat), value = T)

multiples_dat$combined_node_retweeted_display_url<-  apply(multiples_dat[ , nm] , 1 , paste , collapse = "+" )

multiples_dat <- remove_combined_cols(nm, multiples_dat)


#read all the expanded_url
nm <- grep("node_retweeted_status_entities_urls.+_expanded_url$", colnames(multiples_dat), value = T)

multiples_dat$combined_node_retweeted_expanded_url<-  apply(multiples_dat[ , nm] , 1 , paste , collapse = "+" )

multiples_dat <- remove_combined_cols(nm, multiples_dat)

#read all the url
nm <- grep("node_retweeted_status_entities_urls.+_url$", colnames(multiples_dat), value = T)

multiples_dat$combined_node_retweeted_just_url<-  apply(multiples_dat[ , nm] , 1 , paste , collapse = "+" )

multiples_dat <- remove_combined_cols(nm, multiples_dat)


#------------------------------------------------combine the two dataframe together ----------------------------
# now we will just combine the two dataframe together
new_df <- data.frame(distinct_dat, multiples_dat)

#and write it to a file
readr::write_excel_csv(new_df, 'totally_cleaned.csv')

#just read it to make sure, leave it commented
#test <- readr::read_csv("totally_cleaned.csv", stringsAsFactors = F)

#EOF