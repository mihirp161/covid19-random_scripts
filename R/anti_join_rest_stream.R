options(scipen = 9999999, digits = 22)
library(data.table)
library(dplyr)
library(bit64)

#------------------------------------------- file reading and collapsing ----------------------------------------------------


#read only columns that we want
botcsv_data <- data.table::fread("CH_rest_API_unique_userids.csv", select = c("Screenid"))



#--------------------------------------------- remake outputcsv file so you can carry on ---------------------------------------------

#read
outputcsv_file <- data.table::fread("CH_stream_API_unique_userids.csv", select = c("Screenid")) #in linux, just do read.csv("output.csv",stringsAsFactors = F)
colnames(botcsv_data) <- "Screenid"
#now compare the two frame and remove the rows which are in 

outputcsv_file <- dplyr::anti_join(outputcsv_file,botcsv_data, by= c("Screenid"))

#write the file back
data.table::fwrite(outputcsv_file, "new_CH_stream_API_unique_userids.csv") #in linux, do write.csv("new_output.csv")

#EOF