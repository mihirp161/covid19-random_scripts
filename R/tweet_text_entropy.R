options(scipen = 99999, warn = -1, stringsAsFactors = FALSE)
library(rvest)
library(xml2)
library(tidyr)
library(qdapRegex)
library(stringr)
library(stringi)
library(tidyverse)
library(quanteda)
library(purrr)
library(data.table)

# LOOK AT ME!!!
just_original_texts <- 1 # do you want entropy on original tweets only?? 1 for yes or 0 for no

#-------------------------------------------------- #CSV reading ------------------------------------------------------------------
#Read in the csv file
# set the path of where the csv files are and end the file path with / character. In windows swap \ with /
setwd('./entropy files/')

#filenames
files <- list.files( pattern="*.csv$")

#read only columns that we want
#temp <- lapply(files, function(x) data.table::fread(x, select = c("user.screen_name", "retweeted_status.user.screen_name")))
temp <- lapply(files, function(x) readr::read_csv(x))
entro_data <- data.table::rbindlist(temp, fill = T) #make a df

setwd('..') #move the working directory back, or give an specific path

wes_data <- entro_data[sample(.N, 4000)]

readr::write_excel_csv(wes_data, "wes_data.csv")

#(optional, only when you're on your personl computer & specs are low while csv files are big)
#break the large CSV so RAM and Rstudio doesn't crash 
# groups <- (split(entro_data, (seq(nrow(entro_data))-1) %/% 60000)) #here I want 60000 rows per file until last row is reached
# 
# for (i in seq_along(groups)) {
#   write.csv(groups[[i]], paste0("mask_output_MONTH", i, ".csv")) #iterate and write file
# }

#---------------------------------------------- csv texts manipulations --------------------------------------------------------

#remove all the rows that have NA in the text column
entro_data <- entro_data[!is.na(entro_data$tweet_text), ]

#remove all the rows that have NA in the date column
entro_data <- entro_data[!is.na(entro_data$tweet_created_at), ]

#remove the rows if less than the average
entro_data <- entro_data[str_length(entro_data$tweet_created_at) < mean(str_length(entro_data$tweet_created_at)), ]


beautify_texts_in_df <- function(give_me_df){
  
  #make an empty vector to store full texts
  complete_texts <- c()
  
  #loop through each elements of rows
  for(i in seq_along(1:nrow(give_me_df))){
    
    #depending one what's needs we can only perform the analysis on just original tweets or retweets+quoted
    if(just_original_texts == 1){
      
      #check if the tweet begins with RT
      if(!grepl('^RT', give_me_df[i, "tweet_text"])){
        complete_texts <- c(complete_texts, NA) # if yes, assign NA
      }else{
        complete_texts <- c(complete_texts, TRUE) # if not, then it's TRUE, that it's starts with RT
      }
      
    }else if(just_original_texts == 0){
      
      #check if tweet text begins with RT
      if(!grepl('^RT', give_me_df[i, "tweet_text"])){
        complete_texts <- c(complete_texts, give_me_df[i, "tweet_text"]) # if not, then it's original
      }
      else{
        # if text begins with RT then check if quoted_text is NA
        if(!as.logical(is.na(give_me_df[i,"quoted_text"]))){
          # if quoted text is present then append that with retweet
          complete_texts <- c(complete_texts, paste(give_me_df[i, "retweet_text"], give_me_df[i, "quoted_text"], sep=" "))
        }
        else{
          # if quoted text is empty then the text is just a retweet
          complete_texts <- c(complete_texts, give_me_df[i, "retweet_text"])
        }
      }
    }
  }
  if(just_original_texts == 1){
    
    #attach the text
    give_me_df$complete_text <- complete_texts
    
    #only keep the NA columns, because they're original
    give_me_df <- give_me_df[is.na(give_me_df$complete_text), ]
    
    # "fix" the complete vector by copying the original tweets
    give_me_df$complete_text <- give_me_df$tweet_text #<<<<<<<<<<<<<<--------------------------- Don't forget to change this
    
    #return the vector
    return(give_me_df)
    
  }else if(just_original_texts == 0){
    
    #attached the proper texts
    give_me_df$complete_text <- complete_texts
    
    #return the vector
    return(give_me_df)
  }
}

entro_data <- beautify_texts_in_df(entro_data)



#---------------------------------------------- url to title --------------------------------------------------------------------
get_title_tag <- function(url) {
  
  path_to_title <- "/html/head/title"
  
  #sleep for a while
  Sys.sleep(sample(5:8, 1, replace = T))
  
  #if word is not starting with http, return as it is
  if (!startsWith(url, "http")) {
    return("")
  }
  
  #ERROR HANDLING
  possibleError <- tryCatch(
    xml2::read_html(url),
    error=function(e) e
  )
  #errors are caught while reading meaning page is bad or not accessible, return the url
  if(inherits(possibleError, "error")){
    return("")
  }
  else{
    page <- xml2::read_html(url) #read it if good
  }
  #check if title texts exist
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
  else{ #otherwise remove the title tags, and return that
    page <- xml2::read_html(url)
    
    conf_nodes <- rvest::html_nodes(page, xpath = path_to_title)
    
    title <- rvest::html_text(conf_nodes)
    
    return(title)
  }
  
}

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

#---------------------------------------------- custom shannon functions --------------------------------------------------------
# a<- data.frame(text=  c("Everyone who can get a decent mask in ireland should wear one when they leave their houses simple as,the government advice a few weeks ago that they didnt work was plain and simply about trying to insure as many were available as possible for front line workers",
#       "Learn how to avoid getting infected with the novel coronavirus with some common sense ways to avoid catching germs plus tips from a doctor on how to make a homemade medical face mask or a DIY protective face mask"),
#       stringsAsFactors = F)

#compute Shannon entropy
shannon_entropy <- function(target) {
  target <- a$text[1]
  #get the n 
  #freq <- table(target)/length(target)
  freq <- table(strsplit(tolower(target), "\\s+")[[1]])/length(target)
  # vectorize & extract the ns
  vec <- as.data.frame(freq)[ ,2]
  
  #remove 0 avoid NaN from log2
  vec<- vec[vec > 0]
  
  #compute entropy & return
  return(-sum(vec * log2(vec)))
}

#shannon_entropy(a$text[1])


#returns information gain for numerical variables
# e is the computed entropy for this subset
# p is the proportion of records
# n is the number of records in that child

IG_numeric <- function(data, feature, target, bins= 4) {
  
  #Strip out rows where feature is NA
  data <- data[!is.na(data[ ,feature]), ]
  
  #compute shannon_entropy for the parent
  e0 <- shannon_entropy(data[ ,target])
  
  data$cat <- cut(data[ ,feature], breaks= bins, labels= c(1:bins))
  
  #use dplyr to compute e and p for each value of the feature
  dd_data <- data %>% 
             dplyr::group_by(cat) %>% 
             dplyr::summarise(e= shannon_entropy(get(target)), 
                              n= length(get(target)),
                              min= min(get(feature)),
                              max= max(get(feature)),
                              .groups= 'drop')
  
  #calculate p for each value of feature
  dd_data$p <- dd_data$n/nrow(data)
  
  #compute IG
  IG <- e0 - sum(dd_data$p*dd_data$e)
  
  return(IG)
}

#IG_numeric(iris, "Sepal.Length", "Species", bins=5)

#returns information gain categorical variables.
IG_cat <- function(data, feature, target){
  
  #Strip out rows where feature is NA
  data <- data[!is.na(data[ ,feature]), ] 
  
  #use dplyr to compute e and p for each value of the feature
  dd_data <- data %>% 
             dplyr::group_by_at(feature) %>% 
             dplyr::summarise(e= shannon_entropy(get(target)), 
                              n= length(get(target)),
                              .groups= 'drop')
  
  #compute shannon_entropy for the parent
  e0 <- shannon_entropy(data[,target])
  
  #calculate p for each value of feature
  dd_data$p <- dd_data$n / nrow(data)
  
  #compute IG
  IG <- e0 - sum(dd_data$p * dd_data$e)
  
  return(IG)
}



#---------------------------------------------- calculate entropy per document ------------------------------------------------

text_quanteda <- entro_data[, c("tweet_created_at","user_screen_name", "tweet_id_str", "complete_text")]

#remove special characters (like aE^)
text_quanteda$IG_stats <- gsub("[^\x20-\x7E]", " ", text_quanteda$complete_text)

#remove all the special line modiefiers
text_quanteda$IG_stats <- gsub("[\n]", "", text_quanteda$IG_stats)

# if you didn't run the url conversion do this
text_quanteda$IG_stats <- qdapRegex::rm_url(text_quanteda$IG_stats, pattern= qdapRegex::pastex("@rm_twitter_url", "@rm_url"))

#merge all the spaces into single space, remove trailing or leading spaces
text_quanteda$IG_stats <- qdapRegex::rm_white(text_quanteda$IG_stats)

# #get the titles from URLs now
# a <- replace_url_tweet(text_quanteda$IG_stats)
# 
# #put blank for NA words
# a<- gsub("NA", "", a)
# 
# text_quanteda$title_text <- a 
# rm(a)

#remove the urls, otherwise you can run like above again
#text_quanteda$title_text <- qdapRegex::rm_url(text_quanteda$title_text, pattern= qdapRegex::pastex("@rm_twitter_url", "@rm_url"))


#remove everything after @
#qdapRegex::rm_email(text_quanteda$IG_stats)

#remove hastags #
#qdapRegex::rm_tag(text_quanteda$IG_stats)

#remove numbers
#qdapRegex::rm_number(text_quanteda$IG_stats)

#remove dates
#qdapRegex::rm_date(text_quanteda$IG_stats)

#remove asciis
#qdapRegex::rm_non_ascii(text_quanteda$IG_stats)

#remove all punctuation except apostrophes in R
text_quanteda$IG_stats <- gsub("[^[:alnum:][:space:]']", "", text_quanteda$IG_stats)

#------------------
# text_quanteda$IG_stats2 <- text_quanteda$IG_stats
# text_quanteda$IG_stats3 <- text_quanteda$IG_stats
#------------------

#text_quanteda <- text_quanteda[1:500, ]

corp_text_quanteda <- quanteda::corpus(text_quanteda$IG_stats)
dfm_text_quanteda <- quanteda::dfm(corp_text_quanteda, verbose = T) #%>% 
                        #quanteda::dfm_remove(stopwords("english"))

#------------------------------------------- cosine similarity -------------------------------------

(doc_similarity <-  quanteda::textstat_simil(dfm_text_quanteda, method = "cosine", margin = "documents", min_simil = 0.5))

cos_df_simil <- doc_similarity %>% as.data.frame(stringsAsFactors=F) %>% mutate_all(as.character)

repl_docu1_user <- c()
repl_docu2_user <- c()

for(i in 1:nrow(cos_df_simil)){
  # get the usernames associated with the usernames
  repl_docu1_user <- c(repl_docu1_user, text_quanteda$user_screen_name[as.numeric(str_extract(cos_df_simil$document1[i], "[[:digit:]]+"))])
  repl_docu2_user <- c(repl_docu2_user, text_quanteda$user_screen_name[as.numeric(str_extract(cos_df_simil$document2[i], "[[:digit:]]+"))])
  
  #get the texts
  cos_df_simil$document1[i] <- text_quanteda$complete_text[as.numeric(str_extract(cos_df_simil$document1[i], "[[:digit:]]+"))]
  cos_df_simil$document2[i] <- text_quanteda$complete_text[as.numeric(str_extract(cos_df_simil$document2[i], "[[:digit:]]+"))]
  
}

cos_df_simil$document1_user_name <- repl_docu1_user
cos_df_simil$document2_user_name <- repl_docu2_user

cos_df_simil <- cos_df_simil[order(cos_df_simil$cosine, decreasing = TRUE),]

readr::write_excel_csv(cos_df_simil, "cos_similarity.csv")

#dfm_text_quanteda_weighted <- quanteda::dfm_weight(dfm_text_quanteda,scheme = "prop")

#-------
# DOT PLOT TAKE A LOOK AT THIS: http://web.stanford.edu/class/bios221/book/Chap-Graphs.html or this http://www.sthda.com/english/wiki/ggplot2-dot-plot-quick-start-guide-r-software-and-data-visualization

# library(dendextend)

# d1 <- cos_df_simil[1:100,] %>% dist() %>% hclust( method="average" ) %>% as.dendrogram()
# d2 <- cos_df_simil[1:100,] %>% dist() %>% hclust( method="complete" ) %>% as.dendrogram()
# 
# # Custom these kendo, and place them in a list
# dl <- dendlist(
#   d1 %>%
#     set("labels_col", value = c("skyblue", "orange", "grey"), k=3) %>%
#     set("branches_lty", 1) %>%
#     set("branches_k_color", value = c("skyblue", "orange", "grey"), k = 3),
#   d2 %>%
#     set("labels_col", value = c("skyblue", "orange", "grey"), k=3) %>%
#     set("branches_lty", 1) %>%
#     set("branches_k_color", value = c("skyblue", "orange", "grey"), k = 3)
# )
# 
# # Plot them together
# tanglegram(dl,
#            common_subtrees_color_lines = FALSE, highlight_distinct_edges  = TRUE, highlight_branches_lwd=FALSE,
#            margin_inner=7,
#            lwd=2
# )

# 
# library(collapsibleTree)
# library(htmlwidgets)
# library(networkD3)
# 
# collapsibleTree(warpbreaks, c("wool", "tension", "breaks"))
# 
# collapsibleTree(cos_df_simil[1:10,], c("cosine", "document1", "document2"))

#------------------------------------------- entropy ------- -------------------------------------

text_quanteda$IG_stats <- quanteda::textstat_entropy(x= dfm_text_quanteda, margin ="documents", base= 2)$entropy

#features_entropy_pertext <- quanteda::textstat_entropy(dfm_text_quanteda, "features")



#---------------------------------------------
entropy <-  function(s){
  freq <-  prop.table(table(strsplit(s, '')[1]))
  #print(freq)
  -sum(freq * log(freq, base = 2))
}

#------------------------------------- example -------------------------------------------------
# entropy("Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.") # 4.12442 (I added new lines to fit into picture)
# 
# 
# corp_text_quanteda <- quanteda::corpus("HHHHHHHHHHH IIIIIIIII")
# dfm_text_quanteda <- quanteda::dfm(corp_text_quanteda, verbose = T)
# quanteda::textstat_entropy(x= dfm_text_quanteda, margin ="document", base= 2)$entropy
# #Here I get 1. 
# 
# entropy(tolower("HHHHHHHHHHH IIIIIIIII")) # I get 0.9927745, rounded up to 1.
# 
# 
# corp_text_quanteda <- quanteda::corpus("HHHHHHHHHHHIIIIIIIII")
# dfm_text_quanteda <- quanteda::dfm(corp_text_quanteda, verbose = T)
# quanteda::textstat_entropy(x= dfm_text_quanteda, margin ="document", base= 2)$entropy
# # Now here it is 0???

#----------------------------------- old -----------------------------------------------------------
# ig_stats2 <- c() # my own
# 
# for(i in 1:nrow(text_quanteda)){
#   ig_stats2 <- c(ig_stats2, entropy(text_quanteda$IG_stats2[i]))
# }
# 
# text_quanteda$IG_stats2 <- ig_stats2
# 
# ig_stats3 <- c() # mimcing quanteda
# 
# for(i in 1:nrow(text_quanteda)){
#   ig_stats3 <- c(ig_stats3, entropy(tolower(text_quanteda$IG_stats3[i])))
# }
# 
# text_quanteda$IG_stats3 <- ig_stats3

#---------------------------------------------- plot ------------------------------------------

# calculate average entropy per day
text_quanteda$just_date<- as.Date(text_quanteda$tweet_created_at, format ="%a %b %d %H:%M:%S %z %Y", tz = "GMT") 

#------------------------------------------------------ old---------------------------------------
# averages_tbl <- text_quanteda %>% 
#                   dplyr::group_by(just_date) %>% 
#                   dplyr::summarise(avg_info_gain= mean(IG_stats),
#                                    avg_info_gain2= mean(IG_stats2),
#                                    avg_info_gain3= mean(IG_stats3),
#                                    .groups= 'drop')
# 
# colnames(averages_tbl) <- c("just_date", "Quanteda","Defined Entropy","Defined Lower-case")
# averages_tbl <- averages_tbl %>%
#                   tidyr::pivot_longer(-just_date, names_to = "IG_types", values_to = "avg_info_gain")
# 
# 
# # plot a graph: x axis: dates, y axis: entropy/day
# 
# ggplot2::ggplot(averages_tbl, aes(x=just_date , y= avg_info_gain))+
#           geom_path(aes(color = IG_types, linetype = IG_types))+
#           geom_point(aes(color = IG_types, linetype = IG_types))+
#           #geom_line(aes(color = IG_types, linetype = IG_types)) + 
#           scale_x_date(date_labels="%m/%d",date_breaks  ="1 day")+
#           geom_text(aes(label= round(avg_info_gain,1),hjust=0, vjust=0), check_overlap = TRUE)+
#           theme_bw()+
#           theme(plot.title = element_text(hjust = 0.5),
#                 axis.text.x = element_text(angle = 60, vjust = 0.5))+
#           #scale_linetype_manual(values=c("solid", "dotted","longdash"))+
#           #scale_color_manual(labels = c("Quanteda", "Defined Entro.", "Defined + lower-case"), values = c("indianred4", "darkslateblue","gold2")) +
#           labs(title= "Document Stat Freq. between Mar 22- Apr 19", 
#                caption =  "+ based on the Mask query \n * vs quanteda w/ punctuations \n ! done on original tweets",
#                x= "Dates",
#                y= "Avg. Document Entropy",
#                color= "Function Type",
#                linetype= "Function Type") +
#           ggsave("mar22toapr19_docu_entropy_graph.png", height =6, width= 10,units= "in", dpi = 300)
# 
# #readr::write_excel_csv(text_quanteda[,2:5], "mask_document.csv")

#--------------------------------------------------------------------------------------------------------
averages_tbl <- text_quanteda %>% 
  dplyr::group_by(just_date) %>% 
  dplyr::summarise(avg_info_gain= mean(IG_stats),.groups= 'drop')

ggplot2::ggplot(averages_tbl, aes(x=just_date , y= avg_info_gain))+
  geom_path()+
  geom_point()+
  scale_x_date(date_labels="%m/%d",date_breaks  ="1 day")+
  geom_text(aes(label= round(avg_info_gain,1),hjust=0, vjust=0), check_overlap = TRUE)+
  theme_bw()+
  theme(plot.title = element_text(hjust = 0.5),
        axis.text.x = element_text(angle = 60, vjust = 0.5))+
  labs(title= "Document Stat Freq. between Mar 22- Apr 19", 
       caption =  "+ based on the Mask query \n *  \n ! done on original tweets",
       x= "Dates",
       y= "Avg. Document Entropy") +
  ggsave("mar22toapr19_docu_entropy_graph.png", height =6, width= 10,units= "in", dpi = 300)

