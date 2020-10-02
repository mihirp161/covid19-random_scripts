# FOR USF SAIL
# ~Mihir

##-------------------------
#* Use this to extract all the id str from csv
#* the long format will be preserved
##---------------------------

options(scipen = 99999)
library(readr)
library(gmp)

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
  
  # Create "big" numbers by extracting the id_str
  bigA <- c(as.bigz(li[[i]]$id_str))
  
  #Save them as character vector:
  # write them to a txt file
  write.table(data.frame(a=as.character(bigA)), 
              paste0(names[i], ".txt"), 
              row.names=F,col.names=F,quote=FALSE)
}

#EOF