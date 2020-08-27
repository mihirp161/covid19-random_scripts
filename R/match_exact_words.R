# SAIL Labs
# ~Mihir

## Description:
##-------------
#* Due to how we query files, previously with matching the shortest terms in an extct
#* manner then capturing the word...for instance:
#* query_term <- "A B"
#* it would only match with these terms -> "A B C", "A B ;" and not "A", "B"
#* 
#* However the problem is terms such as RX or CIA yielded lot of garbage, and for short
#* terms such as these we have to match exact words, and not the subset. Subset is good
#* because it introduces less bias, but it is also not a good approach when the terms 
#* are smaller hence this script where we take some terms and re-filter some files so
#* we get exact matches.
##-------------

library(tidyverse)
library(data.table)
library(dplyr)
library(stringr)

#read the file
data <- readr::read_csv("fakeTreatments_march_all_usc.csv")

# get a vector of terms that were found
found_terms <- levels(factor(data$matched_str))
found_terms #prints all the terms on the console

#type which term we want to get exact match from, usually very short term yields garbage
which_term <- "rx"

# take a subset of the dataframe
short_term_data <- data %>% 
                    dplyr::filter(matched_str == which_term)

# do the exact matching
rows_with_matches <- data.frame(stringr::str_match(tolower(short_term_data$complete_texts), paste0("\\b",which_term,"\\b")))

#change colname
colnames(rows_with_matches) <- c("found")

#get the indices
rows_with_matches$rownum <- row.names(rows_with_matches)

#remove na rows
rows_with_matches <- rows_with_matches[!is.na(rows_with_matches$found), ]

#now only keep the indices with rownumber
short_term_data <- short_term_data[rows_with_matches$rownum, ]

# now filter the rows that aren't your term
data <- data %>% 
         dplyr::filter(matched_str != which_term)

#attach the two dataframe
data <- dplyr::bind_rows(short_term_data, data)

#write to a csv file
readr::write_excel_csv(data, "fakeTreatments_march_all_usc_redo.csv")

#EOF