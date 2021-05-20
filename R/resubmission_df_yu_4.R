options(scipen = 9999999)
library(tidyverse)

setwd('./done')

file_names <- list.files(recursive = TRUE) 


for(i in file_names){
  assign(i,  readr::read_csv(i))
}

# filtered_df <- dplyr::bind_rows(plandemic_data_march.csv, plandemic_data_april.csv,
#                              plandemic_data_may.csv, plandemic_data_june.csv,
#                              plandemic_data_july.csv, plandemic_data_august.csv)

# setwd("..")
# 
# readr::write_excel_csv(filtered_df %>% dplyr::select(-Screenid), "plandemic_data_march_august_updated.csv")

filtered_df <- dplyr::bind_rows(plandemic_data_may.csv, plandemic_data_june.csv,
                             plandemic_data_july.csv, plandemic_data_august.csv)

setwd("..")

readr::write_excel_csv(filtered_df %>% dplyr::select(-Screenid), "plandemic_data_may_august_updated.csv")



filtered_df <- tibble::add_column(filtered_df, Date_std= as.POSIXct(strptime(filtered_df$Date,
                                                                       "%a %b %d %H:%M:%S +0000 %Y", tz = "GMT")),
                               .after = 1)

filtered_df <- tibble::add_column(filtered_df, Date_std_nm = format(filtered_df$Date_std, "%B"),
                               .after = 2)

filtered_df <- tibble::add_column(filtered_df, Source_joined_std =  as.POSIXct(strptime(filtered_df$Source_joined,
                                                                                 "%a %b %d %H:%M:%S +0000 %Y",
                                                                                 tz = "GMT")),
                               .after = 9)

#check how many unique users are there, because that's how many user will need to have
dplyr::n_distinct(filtered_df$Screenid)

user_part <- filtered_df %>%
               dplyr::group_by(Screenid, Screenname) %>%  
               dplyr::arrange(desc(Date_std)) %>%
               dplyr::slice(1) %>%
               dplyr::select(Screenname,
                             Screenid,
                             Source_followers,
                             Source_following,
                             Source_joined_std)


tally_part <- filtered_df %>% 
                dplyr::group_by(Screenname, Screenid, Date_std_nm) %>%
                dplyr::summarize(Count = n()) %>%
                tidyr::pivot_wider(names_from = Date_std_nm, 
                                   values_from = Count, values_fill = 0)

setwd('./deyu3')

file_names <- list.files(recursive = TRUE) 


for(i in file_names){
  assign(i,  readr::read_csv(i))
}
setwd("..")

bots_df <- dplyr::bind_rows(drYu_ds3_may_bots.csv, drYu_ds3_june_bots.csv,
                             drYu_ds3_july_bots.csv, drYu_ds3_august_bots.csv) %>%
              dplyr::select(user_screen_name,
                            user_user_data_id_str,
                            cap_english,
                            cap_universal) %>%
              dplyr::mutate(user_screen_name = gsub("@","",user_screen_name))  %>%
              dplyr::distinct()

final_df <- dplyr::left_join(tally_part, user_part,  by= c("Screenid", "Screenname"))

final_df <- dplyr::left_join(final_df, bots_df, by= c("Screenid"= "user_user_data_id_str", 
                                                      "Screenname"="user_screen_name")) %>%
              select(User_name= Screenname,
                     User_id= Screenid,
                     Botometer_cap_english= cap_english,
                     Botometer_cap_universal= cap_universal,
                     Twitter_account_created= Source_joined_std,
                     Number_of_followers= Source_followers,
                     Number_of_followees= Source_following,
                     Plandemic_engagement_May= May,
                     Plandemic_engagement_June= June,
                     Plandemic_engagement_July= July,
                     Plandemic_engagement_August= August)#%>%
              #tidyr::drop_na(cap_english, cap_universal)


readr::write_excel_csv(final_df, "Plandemic_Twitter_attributes_051221_file.csv")


