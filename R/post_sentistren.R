#For USF Sail
# ~Mihir

options(scipen=99999)
library(tidyverse)
library(reshape2)
library(viridis)
library(RColorBrewer)


#------------------------------------ this portion is to be used if neutral emotion is need--------------
#read the csv
main_df <- readr::read_csv("temp_with_en_mary_data.csv")

# #remove all the rows that have NA in the text column
# main_df <- main_df[!is.na(main_df$tweet_text), ]
# 
# #remove all the rows that have NA in the date column
# main_df <- main_df[!is.na(main_df$tweet_created_at), ]
# 
# #remove the rows if less than the average
# main_df <- main_df[str_length(main_df$tweet_created_at) < mean(str_length(main_df$tweet_created_at)), ]

# get the date
main_df$just_date <- as.Date(main_df$created_at, format ="%a %b %d %H:%M:%S %z %Y", tz = "GMT") 

#graph the sentiment

focused_ex_tweet <- main_df %>% dplyr::select(positive, negative, neutral, just_date)

# abosulte sentiment
focused_ex_tweet$absolute_sent <- (focused_ex_tweet$positive+abs(focused_ex_tweet$negative))/2

# combined sentiment
focused_ex_tweet$combined_sent <- (focused_ex_tweet$positive+focused_ex_tweet$negative)/2

# combined sentiment
focused_ex_tweet$combined_sent <- (focused_ex_tweet$positive+focused_ex_tweet$negative)/2

#one with neutral emotions included
focused_ex_tweet$absolute_sent_neu <- (focused_ex_tweet$positive+
                                               abs(focused_ex_tweet$neutral)+
                                               abs(focused_ex_tweet$negative))/3

focused_ex_tweet$combined_sent_neu <- (focused_ex_tweet$positive+
                                               focused_ex_tweet$neutral+
                                               focused_ex_tweet$negative)/3



#focused_inc_tweet <- main_df %>% dplyr::select(complete_text,positive, negative, neutral, just_date)

#rm(main_df)


#just keep the combined & absolute
focused_ex_tweet <- focused_ex_tweet %>% dplyr::select(absolute_sent, combined_sent, 
                                                       absolute_sent_neu,combined_sent_neu, just_date)

melted_focused_ex_tweet <- reshape2::melt(focused_ex_tweet, id.vars=c("just_date"))

# 
# combination_tbl <- focused_ex_tweet %>%
#                     dplyr::group_by(positive, negative, neutral, just_date) %>%
#                     dplyr::tally()
# 
# 
# nrow(combination_tbl)


pos<- focused_ex_tweet %>%
        dplyr::group_by(positive,just_date) %>%
        dplyr::tally()

neg <- focused_ex_tweet %>%
        dplyr::group_by(negative,just_date) %>%
        dplyr::tally()

neu <-  focused_ex_tweet %>%
          dplyr::group_by(neutral,just_date) %>%
          dplyr::tally()

abs <- focused_ex_tweet %>%
        dplyr::group_by(absolute_sent,just_date) %>%
        dplyr::tally()

comb <- focused_ex_tweet %>%
        dplyr::group_by(combined_sent,just_date) %>%
        dplyr::tally()

comb_y_neu <- focused_ex_tweet %>%
        dplyr::group_by(combined_sent_neu,just_date) %>%
        dplyr::tally()

abs_y_neu <- focused_ex_tweet %>%
        dplyr::group_by(absolute_sent_neu,just_date) %>%
        dplyr::tally()

# ggplot2::ggplot(melted_focused_ex_tweet, aes(x=just_date, y= variable,fill= factor(value))) + 
#           geom_tile(color='seashell',size = 0.1) +
#           scale_x_date(date_labels="%m/%d",date_breaks  ="1 day")+
#           guides(fill=guide_legend(title="Ratings")) +
#           theme_bw()+
#           scale_fill_viridis_d()+
#           theme(plot.title = element_text(hjust = 0.5),
#                 axis.text.x = element_text(angle = 60, vjust = 0.5))+
#           labs(title= "Sentistrength of tweets between Mar 22- Apr 19",
#                caption =  "+ excluding Apr 01 \n ! done on complete tweets",
#                x= "Dates",
#                y= "Sentiments")+
#           ggsave("sentistren_heatmap.png", height =6, width= 10,units= "in", dpi = 300)

# ggplot2::ggplot() + 
#           geom_line(linetype = "dashed")+
#           geom_point(data=pos, aes(just_date, n, color= factor(positive)),alpha = 0.7)+
#           geom_point(data=neg, aes(just_date, n, color= factor(negative)),alpha = 0.7)+
#           #geom_point(data=neu, aes(just_date, n, color= factor(neutral)))+
#           scale_x_date(date_labels="%m/%d",date_breaks  ="1 day")+
#           theme_bw()+
#           scale_color_brewer(palette = "Paired")+
#           guides(color=guide_legend(title="Ratings")) +
#           theme(plot.title = element_text(hjust = 0.5),
#                 axis.text.x = element_text(angle = 60, vjust = 0.5))+
#           labs(title= "Sentistrength of tweets between Mar 22- Apr 19 (Aggregated)",
#                caption =  "+ excluding Apr 01 \n ! done on complete tweets \n * (pos, neg) only",
#                x= "Dates",
#                y= "Rating Count")+
#         ggsave("dots_sentistren_heatmap.png", height =6, width= 10,units= "in", dpi = 300)


# geom_smooth graph
ggplot2::ggplot(data=melted_focused_ex_tweet, aes(x= just_date, y= value, color= factor(variable))) + 
        geom_smooth(alpha = 0.3)+
        scale_x_date(date_labels="%m/%d",date_breaks  ="2 week")+
        theme_bw()+
        scale_color_brewer(palette = "Paired")+
        guides(color=guide_legend(title="Ratings")) +
        facet_wrap(~variable,
                   scales='free_y',
                   labeller = labeller(variable = c("absolute_sent"= "Absolute Sentiment",
                                                    "combined_sent"= "Combined Sentiment",
                                                    "absolute_sent_neu"="Absolute Sentiment with Neutral Emotion Included",
                                                    "combined_sent_neu"="Combined Sentiment with Neutral Emotion Included")))+
        theme(plot.title = element_text(hjust = 0.5),
              axis.text.x = element_text(angle = 60, vjust = 0.5),
              strip.background =element_rect(fill="azure2"),
              strip.text = element_text(colour = "black"),
              legend.position = "none")+
        labs(title= "Sentistrength on Mary's Data (Trinary Sentistrength)",
             caption =  "+ \n ! done on complete tweets \n * ",
             x= "Dates",
             y= "Value") +
        # scale_color_manual(labels=c("Absolute Sentiment","Combined Sentiment",
        #                             "Absolute Sentiment w/ Neutral Emo.","Combined Sentiment w/ Neutral Emo."),
        #                    values=c("red","green","blue", "yellow"))+
        ggsave("smooth_sentistren_neutral.png", height =6, width= 10,units= "in", dpi = 300)


#----------------------------------- use this portion if scalar emotion is need to be used ----------------
options(scipen=99999)
library(tidyverse)
library(reshape2)
library(viridis)

#read the csv
main_df <- readr::read_csv("temp_with_en_mary_data.csv")

# get the date
main_df$just_date <- as.Date(main_df$created_at, format ="%a %b %d %H:%M:%S %z %Y", tz = "GMT") 

focused_ex_tweet <- main_df %>% dplyr::select(senti_stren_scaled, just_date)

# #----------
#
#remove the neutral
focused_ex_tweet <- focused_ex_tweet[focused_ex_tweet$senti_stren_scaled != 0, ]

#give the tag for each emotion
focused_ex_tweet$sentiment <- ifelse(focused_ex_tweet$senti_stren_scaled>0,
                                     yes = "Positive", no="Negative")

#calculate the frequency for each sentiment
freq <- focused_ex_tweet %>%
        dplyr::group_by(sentiment,just_date) %>%
        dplyr::tally()

#get the mean
freq <- freq %>% group_by(just_date) %>% mutate(proc = (n/sum(n) * 100))


# #-----------
# #break the datafram into postive and negative scaled values
# pos_scaled <- focused_ex_tweet[focused_ex_tweet$senti_stren_scaled>0, ]
# 
# neg_scaled <- focused_ex_tweet[focused_ex_tweet$senti_stren_scaled<0, ]
# 
# pos_scaled$senti_stren_scaled <- pos_scaled$senti_stren_scaled
# neg_scaled$senti_stren_scaled <- neg_scaled$senti_stren_scaled
# 
# pos <- pos_scaled %>%
#         dplyr::group_by(senti_stren_scaled,just_date) %>%
#         dplyr::tally()
# 
# 
# neg <- neg_scaled %>%
#         dplyr::group_by(senti_stren_scaled,just_date) %>%
#         dplyr::tally()
# 
# pos_sl <- pos %>% group_by(just_date) %>% mutate(p1 = mean(senti_stren_scaled))
# 
# neg_sl <- neg %>% group_by(just_date) %>% mutate(p1 = mean(senti_stren_scaled))
# 
# 
# # for line graph for all the RATINGS of emotion
# ggplot2::ggplot() +
#         geom_line(data=pos, aes(just_date, n, color= factor(senti_stren_scaled)),alpha = 0.7)+
#         geom_line(data=neg, aes(just_date, n, color= factor(senti_stren_scaled)),alpha = 0.7)+
#         scale_x_date(date_labels="%m/%d",date_breaks  ="1 month")+
#         theme_bw()+
#         scale_color_brewer(palette ="Paired", direction=-1)+
#         guides(color=guide_legend(title="Emotion")) +
#         theme(plot.title = element_text(hjust = 0.5),
#               axis.text.x = element_text(angle = 60, vjust = 0.5))+
#         labs(title= "Sentistrength on Mary's Data (Scalar Sentistrength)",
#              caption =  "+  \n ! done on complete tweets \n * ",
#              x= "Dates",
#              y= "Value") +
#         ggsave("line_sentistren_pos_neg.png", height =6, width= 10,units= "in", dpi = 300)
# 
# 
# # this is for aggregated sentimemt
# ggplot2::ggplot() +
#         geom_line(data=pos_sl, aes(just_date, p1),alpha = 0.7,color= "green")+
#         geom_line(data=neg_sl, aes(just_date, abs(p1)),alpha = 0.7, color= "red")+
#         scale_x_date(date_labels="%m/%d",date_breaks  ="1 month")+
#         theme_bw()+
#         ylim(1.0, 5.0)+
#         guides(color=guide_legend(title="Emotion")) +
#         theme(plot.title = element_text(hjust = 0.5),
#               axis.text.x = element_text(angle = 60, vjust = 0.5))+
#         labs(title= "Sentistrength on Mary's Data (Scalar Sentistrength)",
#              caption =  "+  \n ! done on complete tweets \n * ",
#              x= "Dates",
#              y= "Value") +
#         ggsave("mean_sentistren_pos_neg.png", height =6, width= 10,units= "in", dpi = 300)
# 
# # counts the tallied
# ggplot2::ggplot(data=freq, aes(x= just_date, y= proc/100, color= factor(sentiment))) + 
#         geom_line(alpha = 0.7)+
#         scale_x_date(date_labels="%b-%d",date_breaks  ="1 month")+
#         scale_y_continuous(labels = scales::percent)+
#         theme_bw()+
#         scale_color_manual(values = c("darkred", "forestgreen"))+
#         guides(color=guide_legend(title="Sentiment")) +
#         # facet_wrap(~variable,
#         #            scales='free_y',
#         #            labeller = labeller(variable = c("senti_stren_scaled"= "Scaled Sentiment")))+
#         theme(plot.title = element_text(hjust = 0.5),
#               axis.text.x = element_text(angle = 60, vjust = 0.5),
#               # strip.background =element_rect(fill="azure2"),
#               # strip.text = element_text(colour = "black")
#               )+
#         labs(title= "Sentistrength on Mary's Data (Scalar Sentistrength)",
#              caption =  "+done with proportion  \n ! done on complete tweets \n * ",
#              x= "Dates",
#              y= "Sentiment per tweet")+
#         ggsave("prop_sentistren_pos_neg.png", height =6, width= 10,units= "in", dpi = 300)

#-----------------------------------------------------------


melted_focused_ex_tweet <- reshape2::melt(focused_ex_tweet, id.vars=c("just_date"))


# geom_smooth graph (single graph)
ggplot2::ggplot(data=melted_focused_ex_tweet, aes(x= just_date, y= value, color= factor(variable))) + 
        geom_smooth(alpha = 0.3)+
        scale_x_date(date_labels="%m/%d",date_breaks  ="1 day")+
        theme_bw()+
        scale_color_brewer(palette = "Paired")+
        guides(color=guide_legend(title="Ratings")) +
        # facet_wrap(~variable,
        #            scales='free_y',
        #            labeller = labeller(variable = c("senti_stren_scaled"= "Scaled Sentiment")))+
        theme(plot.title = element_text(hjust = 0.5),
              axis.text.x = element_text(angle = 60, vjust = 0.5),
              strip.background =element_rect(fill="azure2"),
              strip.text = element_text(colour = "black"),
              legend.position = "none")+
        labs(title= "Sentistrength on Mary's Data (Scalar Sentistrength)",
             caption =  "+  \n ! done on complete tweets \n * ",
             x= "Dates",
             y= "Value") +
        # scale_color_manual(labels=c("Absolute Sentiment","Combined Sentiment",
        #                             "Absolute Sentiment w/ Neutral Emo.","Combined Sentiment w/ Neutral Emo."),
        #                    values=c("red","green","blue", "yellow"))+
        ggsave("smooth_sentistren_scalar.png", height =6, width= 20,units= "in", dpi = 300)



