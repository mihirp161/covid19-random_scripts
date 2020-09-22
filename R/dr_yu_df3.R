#FOR USF SAIL
# ~Mary, Dre, Mihir

## Description:
##-------------
#* We can use this file to generate new csv structure (no. 3) for Dr Yu
##-------------

options(scipen = 99999)
library(rio)
library(data.table)
library(dplyr)
library(readr)
library(plyr)
library(tidyr)

#set where the Dr Yu's xlsx is
setwd('./yu_original_users/')

# read the xlsx file from dr Yu
xls_list <- rio::import_list("nodal attributes (091720).xlsx", setclass = "tbl")

#bring in th user list
users <- xls_list$`Twitter user list(N=148)`


#---------------------------------------- read all the users folder -----------------------

# first read all the users files
setwd('./users/')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
temp <- lapply(files, function(x) readr::read_csv(x))# fread was behaving strange with integer64

usercsv_data <- data.table::rbindlist(temp, fill = T) #make a df

#subset
usercsv_data <- usercsv_data %>% 
                    dplyr::select("user.id_str",
                                   "user.screen_name",
                                   "user.location",
                                   "user.verified",
                                   "user.created_at",
                                   "user.followers_count",
                                   "user.friends_count")

setwd('..')
#---------------------------------------- read all the filtered folder -----------------------

# first read all the users files
setwd('./term_filtered/')

#filenames
files <- list.files( pattern="*.csv$")

#read
temp <- lapply(files, function(x) readr::read_csv(x))
filteredcsv_data <- data.table::rbindlist(temp, fill = T) #make a df

filteredcsv_data <- filteredcsv_data %>% 
                     dplyr::select("user.id_str", "user.screen_name")


#------------------------------------------- make an empty column so we can fill later -------------
cols <- c(plandemic_count= NA_real_,
          BotOrNot= NA_real_)

# join the plandemic and users

usercsv_data$user.id_str <- as.character(usercsv_data$user.id_str)
filteredcsv_data$user.id_str <- as.character(filteredcsv_data$user.id_str)

#------------------------------------------ joining tables with relations ---------------------------

#paste the @ in front of screen names
filteredcsv_data$user.screen_name <- paste0("@", filteredcsv_data$user.screen_name)
usercsv_data$user.screen_name <- paste0("@", usercsv_data$user.screen_name)


#subset the plandemic table based on 148 users
filteredcsv_data <- filteredcsv_data[filteredcsv_data$user.screen_name %in% users$users, , drop = FALSE]


# choose the highest pair (follower_count, favourite_count) for that user in that month
usercsv_data <- usercsv_data %>%
                    dplyr::group_by(user.id_str, user.screen_name) %>%
                    dplyr::slice(which.max(user.friends_count + user.followers_count))

# now join the table
right_shell_df <- dplyr::left_join(filteredcsv_data,usercsv_data,  by= c("user.id_str", "user.screen_name"))

#put the blank columsn
right_shell_df<- tibble::add_column(right_shell_df, !!!cols[dplyr::setdiff(names(cols), names(right_shell_df))])

#---------------------------------------------- cut the data frame with users in Dr Yus table -----------------

# set the save directory
setwd('./save_directory/')

# count the rows for each user
right_shell_df <- right_shell_df %>%
                    dplyr::group_by(user.screen_name) %>%
                    plyr::count() %>%
                    dplyr::mutate(plandemic_count = freq) %>%
                    dplyr::select(-freq, -user.id_str)

#write to excel
readr::write_excel_csv(right_shell_df, "drYu_ds3_month.R")

#EOF