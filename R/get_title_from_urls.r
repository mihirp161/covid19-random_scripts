# For USF SAIL
# ~Wesley, Mihir

options(scipen = 9999, warn = -1)
library(rvest)
library(dplyr)
library(xml2)
library(tidyr)
library(qdapRegex)
library(purrr)
library(stringr)

get_title_tag <- function(url) {
  
  path_to_title <- "/html/head/title"
  
  if (!startsWith(url, "http")) {
    return("")
  }
  
  #ERROR HANDLING
  possibleError <- tryCatch(
    xml2::read_html(url),
    error=function(e) e
  )

  if(inherits(possibleError, "error")){
    return("")
  }
  else{
    page <- xml2::read_html(url)
  }
  
  if(identical(page %>%
                    rvest::html_nodes(xpath = path_to_title) %>%
                    unique(), character(0))){
    return("")
  }
  else if(identical(page %>%
                    rvest::html_nodes(xpath = path_to_title) %>%
                    rvest::html_text() %>%
                    unique(), character(0))){
    return("")
  }
  else{
    page <- xml2::read_html(url)

    conf_nodes <- rvest::html_nodes(page, xpath = path_to_title)

    title <- rvest::html_text(conf_nodes)

    return(title)
  }

}

raw_output <- read.csv("output-2020-01-21.csv", stringsAsFactors = FALSE)
raw_output$text <- gsub("[^\x20-\x7E]", "", raw_output$text)
raw_output$text <- gsub("[\n]", "", raw_output$text) 

#main function that replaces URL
replace_url_tweet <- function(any_text_column){
  
  #vector that holds each row
  then_this <- c()

  for(x in seq_along(any_text_column)){
    a <- strsplit(any_text_column[x],split=" ") #list of words
      
    v <- c() #stores the return from helper function
    for(i in a){
      for(j in i){
        v <- c(v, get_title_tag(j)) # get the title
      }
    }
    #converts toa vector
    k <- unlist(a)
    #check where we have a title returned
    k[which(startsWith(k, "http") %in% TRUE)] <- NA
    #replace the "" with NA
    v <- replace(v, v=="", NA)
    #make a dataframe
    df <- data.frame(v= v, k = k, stringsAsFactors = FALSE)
    #merge
    replace_with_this <- df %>% 
                          dplyr::mutate(v = ifelse(is.na(v), k, v)) %>% 
                          dplyr::select(v)
    #append
    then_this <- c(then_this,paste((replace_with_this[,1]),collapse=" "))
  }
  
  return(then_this)
}

a <- replace_url_tweet(raw_output$text)
a<- gsub("NA", "", a)
raw_output$title_text <- a 
rm(a)

#remove the urls, otherwise you can run like above again.
raw_output$title_text <- qdapRegex::rm_url(raw_output$title_text, pattern= qdapRegex::pastex("@rm_twitter_url", "@rm_url"))

write.csv(raw_output, "21_jan_file.csv")
