#For USF Sail
# ~Mihir

options(scipen=99999)
library(tidyverse)
library(reshape2)
library(vviridis)

#read the csv
main_df <- readr::read_csv("temp_with_en_no_mask.csv")

#remove all the rows that have NA in the text column
main_df <- main_df[!is.na(main_df$tweet_text), ]

#remove all the rows that have NA in the date column
main_df <- main_df[!is.na(main_df$tweet_created_at), ]

#remove the rows if less than the average
main_df <- main_df[str_length(main_df$tweet_created_at) < mean(str_length(main_df$tweet_created_at)), ]

# get the date
main_df$just_date <- as.Date(main_df$tweet_created_at, format ="%a %b %d %H:%M:%S %z %Y", tz = "GMT") 

#graph the sentiment

focused_ex_tweet <- main_df %>% dplyr::select(positive, negative, neutral, just_date)
focused_inc_tweet <- main_df %>% dplyr::select(complete_text,positive, negative, neutral, just_date)

rm(main_df)

melted_focused_ex_tweet <- reshape2::melt(focused_ex_tweet, id.vars=c("just_date"))

combination_tbl <- focused_ex_tweet %>%
                    dplyr::group_by(positive, negative, neutral, just_date) %>%
                    dplyr::tally()

nrow(combination_tbl)

pos<- focused_ex_tweet %>%
        dplyr::group_by(positive,just_date) %>%
        dplyr::tally()

neg <- focused_ex_tweet %>%
        dplyr::group_by(negative,just_date) %>%
        dplyr::tally()

neu <-  focused_ex_tweet %>%
          dplyr::group_by(neutral,just_date) %>%
          dplyr::tally()

ggplot2::ggplot(melted_focused_ex_tweet, aes(x=just_date, y= variable,fill= factor(value))) + 
          geom_tile(color='seashell',size = 0.1) +
          scale_x_date(date_labels="%m/%d",date_breaks  ="1 day")+
          guides(fill=guide_legend(title="Ratings")) +
          theme_bw()+
          scale_fill_viridis_d()+
          theme(plot.title = element_text(hjust = 0.5),
                axis.text.x = element_text(angle = 60, vjust = 0.5))+
          labs(title= "Sentistrength of tweets between Mar 22- Apr 19",
               caption =  "+ excluding Apr 01 \n ! done on complete tweets",
               x= "Dates",
               y= "Sentiments")+
          ggsave("sentistren_heatmap.png", height =6, width= 10,units= "in", dpi = 300)

ggplot2::ggplot() + 
          geom_line(linetype = "dashed")+
          geom_point(data=pos, aes(just_date, n, color= factor(positive)),alpha = 0.7)+
          geom_point(data=neg, aes(just_date, n, color= factor(negative)),alpha = 0.7)+
          #geom_point(data=neu, aes(just_date, n, color= factor(neutral)))+
          scale_x_date(date_labels="%m/%d",date_breaks  ="1 day")+
          theme_bw()+
          scale_color_brewer(palette = "Paired")+
          guides(color=guide_legend(title="Ratings")) +
          theme(plot.title = element_text(hjust = 0.5),
                axis.text.x = element_text(angle = 60, vjust = 0.5))+
          labs(title= "Sentistrength of tweets between Mar 22- Apr 19 (Aggregated)",
               caption =  "+ excluding Apr 01 \n ! done on complete tweets \n * (pos, neg) only",
               x= "Dates",
               y= "Rating Count")+
        ggsave("dots_sentistren_heatmap.png", height =6, width= 10,units= "in", dpi = 300)


