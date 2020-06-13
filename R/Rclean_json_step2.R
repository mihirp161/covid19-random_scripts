#For USF SAIL
#~Mihir

#For USF SAIL
#~Mihir

library(readr)
library(dplyr)

#-------- UPDATE OUT FILE NAMES & PATH HERE!!!--------------#

#directory name where you want to storage output files
out_fold_name <- "/home/d/ddyson/Desktop/100K_March_ORIGINAL_TRY2" # <---- Here

#output csv name
out_csv_name<- 'USC_march_100K_file_final'

#Paths
all_months_csv_folder <- "/work_bgfs/d/ddyson/re-made csvs from remade jsonls"
bots_csv_folder <- "/work_bgfs/d/ddyson/USC_MARCH_STUFF/all_bots_scores"
rand_100k_usernames_folder <- "/work_bgfs/d/ddyson/USC_MARCH_STUFF/rand_100k_screen_names"
#-----------------------------------------------------------#

#make folder, if exist kill the terminal
if(dir.exists(out_fold_name)){
  print("Directory exists. Please change both folders names in this block to whatever.")
  quit()
}else{
  dir.create(out_fold_name)
}

#-----------------------------------------------------------#

#read all the csvs
files <- setwd(all_months_csv_folder) #set path
files <- list.files(pattern="*.csv") #get the files
csvs_tbl <- lapply(files, readr::read_csv) %>% dplyr::bind_rows() #extract data and make dataframe

#read all the bot scores
files <- setwd(bots_csv_folder)
files <- list.files(pattern="*.csv")
bots_tbl <- lapply(files, readr::read_csv) %>% dplyr::bind_rows()

#read the 100K random file
files <- setwd(rand_100k_usernames_folder)
files = list.files(pattern="*.csv")
Rand100K_tbl <- lapply(files, readr::read_csv) %>% dplyr::bind_rows()

#--->First, filter the main csv based on the 100K file. We want to keep the user who have been processed though botometer
csv_tbl_filtered <- csvs_tbl %>%
                      dplyr::filter(user_screen_name %in% Rand100K_tbl$user_screen_name)

#---------------------------------X remove the variables we are not using
rm(csvs_tbl,Rand100K_tbl)

#--->Second, attach the botometer df to the filtered one, keep the ones that returned NA in Botometer

csv_tbl_filtered_w_bot <- dplyr::left_join(csv_tbl_filtered, bots_tbl, by= "user_screen_name") # Or inner_join??

#--------------------------------X remove furthervariable
rm(csv_tbl_filtered,bots_tbl)

#--------------------------------X Here you can select the columns you need before writing

#comment out the columns we don't need
select_columns_here <- c(
                          "tweet_created_at",              
                          "tweet_id_str",                     
                          "tweet_text",                       
                          "in_reply_to_status_id_str",        
                          "in_reply_to_user_id_str",          
                          "in_reply_to_screen_name",
                          "tweets_retweet_count",
                          "tweets_favourite_count",
                          "retweet_created_at",
                          "retweet_id_str",
                          "retweet_text",
                          "retweets_retweet_count",
                          "retweets_favourite_count",
                          "user_id_str",
                          "user_screen_name",
                          "user_description",
                          "user_location",
                          "user_coordinates",
                          "user_followers_count",
                          "user_friends_count",
                          "user_listed_count",
                          "user_statuses_count",
                          "user_favourites_count",
                          "user_profile_creation_at",
                          "retweets_user_id_str",
                          "retweets_user_screen_name",
                          "retweets_user_description",
                          "retweets_user_location",           
                          "retweets_user_coordinates",        
                          "retweets_user_followers_count",    
                          "retweets_user_friends_count",      
                          "retweets_user_listed_count",       
                          "retweets_user_statuses_count",     
                          "retweets_user_favourites_count",   
                          "retweets_user_retweet_count",      
                          "retweets_user_profile_creation_at",
                          "mentions_screen_names",            
                          "mentions_user_id_str",             
                          "retweet_mentions_screen_names",    
                          "retweet_mentions_user_id_str",     
                          "quoted_mentions_screen_names",     
                          "quoted_mentions_user_id_str",      
                          "quoted_created_at",                
                          "quoted_id_str",                    
                          "quoted_text",                      
                          "quoted_retweet_count",             
                          "quoted_favourite_count",           
                          "quoted_user_id_str",               
                          "quoted_user_screen_name",          
                          "quoted_user_description",          
                          "quoted_user_location",             
                          "quoted_user_coordinates",          
                          "quoted_user_followers_count",      
                          "quoted_user_friends_count",        
                          "quoted_user_listed_count",         
                          "quoted_user_favourites_count",     
                          "quoted_user_statuses_count",       
                          "user_verified",                    
                          "retweet_user_verified",            
                          "quoted_user_verified",             
                          "user_place_name",                  
                          "retweet_user_place_name",          
                          "quote_user_place_name",            
                          "user_country_code",                
                          "retweet_user_country_code",        
                          "quote_user_country_code",          
                          "display_scores_english",           
                          "display_scores_universal",         
                          "cap_english",                      
                          "cap_universal"
                        )

csv_tbl_filtered_w_bot <- csv_tbl_filtered_w_bot[ ,select_columns_here]

#Finall write to a file
readr::write_excel_csv(csv_tbl_filtered_w_bot, paste0(out_fold_name,"/", out_csv_name,"_utf8.csv"))
