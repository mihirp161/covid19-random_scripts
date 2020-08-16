# Dre Dyson
# Contact ergosumdre@gmail.com for details

# This script will ulitize University of South Florida's CIRCE parallel computing cluster 
# to simply query tweet tables given provided keywords

# dir <- location of stored csv files
# keywords <- keywords needed to be queried
keywordMatching <- function(dir, keywords){ # Function in itself uses parallel computing so no need to implement into other code
  files <- list.files(dir, full.names = TRUE, pattern = "*.csv") # create var of files in dir
  data <- parallel::mclapply(files, data.table::fread, integer64 = "character", mc.cores = 6)# (parallel::detectCores()-1)) #suitable for local computing. Also applicable for CIRCE's resources
  message("read in dataset...") # Since are dealing with a large volume of data, we should include a progess indicator
  df <- do.call(rbind,data) # merge list into data frame
  mDf <- ifelse(is.na(df$extended_tweet.full_text),  # insures we are not using truncated columns 
                yes = ifelse(is.na(df$retweeted_status.extended_tweet.full_text), 
                             yes = df$text, 
                             no = df$retweeted_status.extended_tweet.full_text),
                no = df$text)
  library(DescTools)
  keywordList <- paste0("%", keywords, "%") # create var of broad keywords. ie..if a keyword has 'mask' it will match against 'masks', 'facemask', 'masked'
  message("...matching keywords") # Since are dealing with a large volume of data, we should include a progess indicator
  return(df[which(mDf %like% keywordList),]) # indexes and returns only matched columns
}

# Testing functionality
faceMaskTweets <- keywordMatching("data", "masks")

#EOF