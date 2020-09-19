# For SAIL LAB
# ~Presentation, Mihir

## Description:
##-------------
#* Use this file to sample % amount of rows frm categorial variable.
##-------------

library(dplyr)
library(rio)
library(data.table)
#library(stringi)

#imports the excel file (don't forget to set path for the file)
df <- rio::import_list("DGRL15.5.merged.xlsx", setclass = "tbl")

# here i selected a dataframe, then only picked out columns of intrest 
file1<- df$Sheet1 %>% dplyr::select(year, `document title`, Query)

#STEP 1: Remove the duplicates documents title

# dplyr way works (Thanks Mary Falling)!
# dplyr way (more readable)
file1<- file1 %>%
          dplyr::distinct(`document title`, .keep_all = TRUE) %>% #remove duplicates from title, but keep the dataframe intact
          dplyr::arrange(dplyr::desc(`document title`)) #sort the df in descending order

#OR
# data table way (less readable)
# set the datatable, and sort the whole df in alphabetic order, then keep only the unique ones by using title column

file1<- unique(setDT(file1)[order(`document title`)], by = "document title")

# write this to a xlsx file
rio::export(file1, file= "step1_newDGRL15.5.merged.xlsx")

# STEP 2: Select the sample rows from each dataframe
# lapply takes a list of dataframe which are split on the categorical variable
# then at each iteration it will pass one dataframe to a custom function.
# In the custom function, you take the dataframe then you use the sample function to select % portions
# recall you without , in the [] means you are sampling whole dataframe
file2_1 <- lapply(split(file1, file1$Query),
              function(df_inp) subdf[sample(1:nrow(df),
                                           size= ceiling(0.10*nrow(df)),
                                           replace = F),
                                    ]
            )

#becase lapply gave you a list, you can use rbind function or (bind_rows from dplyr) to make the list into a datafram
# you would need a do.call method to iterate through each list of categories/queries
file2_1 <- do.call('rbind', file2_1) 

rio::export(file2_1, file= "step2_newDGRL15.5.merged.xlsx")

#-------------------------------------------Ignore, not part of the solution ---------------------------------

# #STEP 1
# file1 <- df$Sheet1 %>%
#   dplyr::group_by(`document title`) %>%
#   dplyr::summarise(years = paste(year, collapse=", "),
#               relenvancy= paste(year, collapse=", "),
#               data= paste(Data, collapse=", "),
#               `applied techniques`= paste(`Techniques applied`, collapse=", "),
#               `software tools`= paste(`Software tools`, collapse=", "),
#               type= paste(`Descriptive/Predictive/Tool building/Hypothesis testing/framework/design science`, collapse=", "),
#               goal= paste(Goal, collapse=", "),
#               `subject domain`= paste(`Subject Domain`, collapse=", "),
#               discipline= paste(Discipline, collapse=", "),
#               notes= paste(note, collapse=", "),
#               query= paste(Query, collapse=", "))
# 
# 
# rio::export(file1, file= "step1_newDGRL15.5.merged.xlsx")
# 
# #STEP 2
# multiple_query <- file1[stringi::stri_detect_fixed(file1$query,c(",")), ]
# 
# distinct_query <- file1[!stringi::stri_detect_fixed(file1$query,c(",")), ]
# 
# set.seed(786)
# file2_1 <- lapply(split(distinct_query, distinct_query$query),
#               function(subdf) subdf[sample(1:nrow(subdf),
#                                            size= ceiling(0.10*nrow(subdf)), 
#                                            replace = F), 
#                                     ]
#           )
# 
# file2_1 <- do.call('rbind', file2_1)
# 
# 
# file2_2 <- lapply(split(multiple_query, multiple_query$query),
#                 function(subdf) subdf[sample(1:nrow(subdf),
#                                              size= 1, 
#                                              replace = F), 
#                                       ]
#             )
# 
# file2_2 <- do.call('rbind', file2_2)
# 
# 
# file2 <- dplyr::bind_rows(file2_1, file2_2)
# 
# rio::export(file2, file= "step2_newDGRL15.5.merged.xlsx")