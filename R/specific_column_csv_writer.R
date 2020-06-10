# For USF SAIL
# ~Mihir

options(warn = -1)
library(readr)

#input csv name (Needs to be updated everytime)
usv_in_file_name <-"C:/Users/ThinkPad/PycharmProjects/jsonstuff/venv/100K_March_NO_URL_ORIGINAL/USC_march_100K_file_final_utf8.csv"

#output csv name (Needs to be updated everytime)
ucsv_out_file_name <- "filtered_USC_march_100K_file-utf8.csv"

#ignore the warning, we only care about the column names
#colnames_of_df <- colnames(read.csv(usv_file_name, stringsAsFactors = F, nrow=1, fileEncoding = "UTF-8-BOM"))

#select colnames
#please comment the ones you want to omit
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

#based on the columns we have selected, we will write only the ones a analsis person needs. SAVE SPACE, SAVE EARTH!

#read with readr, it's defaul encoding is bit better to handle & error prone
give_user_this <- readr::read_csv(file = usv_in_file_name) #we could have used this, but it's better to store here

#write with a columns person required
readr::write_excel_csv(give_user_this, ucsv_out_file_name)


