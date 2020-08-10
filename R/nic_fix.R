# For USF SAIL
# ~Mihir

options(scipen = 9999)
library(qdapRegex)
library(tidyverse)
library(stringr)
library(stringi)

#---------------------------------- read the file---------------------------------------

this_path <- 'C:\\Users\\ThinkPad\\Downloads\\Conspiracy Theory-selected\\fixThese'
setwd(this_path)

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))

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
  
  setwd('..')
  
  #overwrite
  readr::write_excel_csv(df, files[file_i])
  
  file_i <- file_i+1 #increment the file name
  
  setwd(this_path)
}

#EOF