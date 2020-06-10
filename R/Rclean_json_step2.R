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

#-----X remove the variables we are not using
rm(csvs_tbl,Rand100K_tbl)

#--->Second, attach the botometer df to the filtered one, keep the ones that returned NA in Botometer

csv_tbl_filtered_w_bot <- dplyr::left_join(csv_tbl_filtered, bots_tbl, by= "user_screen_name") # Or inner_join??

#-----X remove furthervariable
rm(csv_tbl_filtered,bots_tbl)

#Finall write to a file 
readr::write_excel_csv(csv_tbl_filtered_w_bot, paste0(out_fold_name,"/", out_csv_name,"_utf8.csv"))
