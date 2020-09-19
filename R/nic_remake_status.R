# SAIL Labs
# ~Mihir

## Description:
##-------------
#* after we have hydrated the files to get all the retweet counts and favorite counts, 
#* we can use this file to join the columns. You will need the queried csv files, and 
#* hydrated csv file which was convereted using some table file.
##-------------

options(scipen = 9999)
library(qdapRegex)
library(tidyverse)
library(stringr)
library(stringi)
library(data.table)

#---------------------------------- read the file---------------------------------------

this_path <- 'all_the_queried_filtered_files_location'
setwd(this_path)

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))



#read the status file
x <- readr::read_csv("rested_csv_file_complete.csv") 

#keep only columns
x <-  x %>% dplyr::select(id_str, retweet_count, favorite_count)

file_i <- 1

for(t in temp){
  
  df <- t
  
  #----------------------------------- all the fixes-------------------------------------
  #attach hashtags
  df$hashtags <- sapply(qdapRegex::rm_hash(df$complete_texts, extract = T), paste0, collapse=";")
  
  #replace "NA" with NA
  df$hashtags <- gsub('NA', NA, df$hashtags)
  
  
  #attach mentions by grouping all the texts
  df$mentions_screen_name <- sapply(stringr::str_extract_all(df$complete_texts,
                                                             "@[[:alnum:]_]+"), paste0, collapse=";")
  
  #replace " " with NA
  df$mentions_screen_name <- gsub(' ', NA, df$mentions_screen_name)
  
  #remove the last instance of ";" with "" in mention id_str
  df$mentions_id_str <- stringi::stri_replace_last_fixed(df$mentions_id_str , ';', '')
  
  # data.table way of merging the column
  setDT(df)
  setDT(x)
  
  #join and replace the existing columns with same name. Newly added columns are updated
  df[x, on = .(id_str), `:=`(retweet_count = i.retweet_count, favorite_count = i.favorite_count)]
  
  #remove rows that are NA on complete text
  df <- df[!is.na(df$complete_texts), ]

  setwd('..')
  
  #overwrite
  readr::write_excel_csv(df, files[file_i])
  
  file_i <- file_i+1 #increment the file name
  
  setwd(this_path)
}


#EOF
