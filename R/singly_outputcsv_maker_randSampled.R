# SAIL Labs
# ~Mihir

## Description:
##-------------
#* This file will read all the csv, select the user.screen_name and keeps the unique usernames only.
#*  Then you can specify people and number of rows you want to keep in each file. It is meant to
#*  help with botometer.
##-------------


library(data.table)

# first read all the files in a folder (Give the path to folder (....), ending /)
setwd('./path_to_files/')

#filenames
files <- list.files( pattern="*csv$")

#read only columns that we want
temp <- lapply(files, function(x) data.table::fread(x, select = c("user.screen_name")))
data <- data.table::rbindlist(temp, fill = T) #make a df


#remove the duplicates usernames
n_original <- data[!duplicated(data$user.screen_name), ]

#sample 100K with probablity sample without replacements
set.seed(786)
rand_data <- data.frame(user.screen_name= sample(n_original$user.screen_name, 100000, replace = F), stringsAsFactors = F)


# write a file for letter
data.table::fwrite(rand_data, "original_user_name_file.csv")

# Here I want 20000 rows per file until last row is reached
# this method forces the execution from innermost, outer () opttional
groups <- (split(rand_data, (seq(nrow(rand_data))-1) %/% 20000))
people <- c("1_user", "2_user", "3_user", "4_user", "5_user")

#loop through end of each split, and write file with i
for (i in seq_along(groups)) {
  write.csv(groups[[i]], paste0("output_", people[i], ".csv")) 
}


#all files would be in the current directory which got set all the way above.
#EOF
