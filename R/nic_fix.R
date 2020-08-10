# For USF SAIL
# ~Mihir

options(scipen = 9999)
library(qdapRegex)
library(tidyverse)
library(stringr)
library(stringi)

#---------------------------------- read the file---------------------------------------
this_file <- "drnic_reop_hasReop_march.csv"

df <- readr::read_csv(this_file)

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

#overwrite
readr::write_excel_csv(df, this_file)

#EOF