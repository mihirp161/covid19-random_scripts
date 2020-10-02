# FOR USF SAIL
# ~Mihir

##-------------------------
#* Use this to write a url column in all the csvs
##---------------------------

options(scipen = 99999)
library(readr)

# set where the csv files are
setwd('.//')

#get all the csv names
files <- list.files(pattern = "*.csv")

# Create list of data frame names without the ".csv" part 
names <- sub('\\..*$', '', files)

# # read all the dataframe in their own lists
# for(i in names){
#   assign(i, readr::read_csv(paste(i, ".csv", sep="")))
# }

# actually make a list of dataframe
li <- lapply(files, readr::read_csv)

# write all the dataframe to text
for(i in 1:length(li)){
  
  # Create a url column and make the links for urls
  li[[i]]$tweet_url <- paste0("www.twitter.com/",
                              li[[i]]$user.screen_name,
                              "/status/", li[[i]]$id_str)
  
  # write them to a csv file
  readr::write_csv(li[[i]], paste0(names[i], "_tweet_urls.csv"))
}

#EOF