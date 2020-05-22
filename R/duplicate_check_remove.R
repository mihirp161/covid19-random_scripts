#For USF SAIL
#~Mihir 

library(data.table)
library(dplyr)
options(scipen = 9999)

#goes together with python file text_file_gatherer.py 
# first read all the files in USC month folder (Give the path to folder, ending /)
setwd('./2020-03/')

#filenames
files <- list.files( pattern="*txt$")

for(i in seq_along(files)){
  vocabulary <- read.table(files[i], header=F, colClasses="numeric")
  write.table(x= vocabulary %>% distinct(V1, .keep_all = T),
              file = files[i], sep="\n",col.names = F, row.names = F)
}

setwd('..') #goes back once

#EOF


