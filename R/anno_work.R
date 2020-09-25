#FOR USF SAIL
# ~Mihir

## Description:
##-------------
#* This file generates IRR vs other annotators confidence
##-------------

library(ggplot2)
library(dplyr)
library(plyr)
library(tidyr)
library(ggridges)

anno_df <- readr::read_csv("annotations_fullIRR_botAttached.csv")

anno_df<- anno_df[!is.na(anno_df$confidence_score),]

# #remove alex
# anno_df <- anno_df[anno_df$name != "Alex", ]
#
# z score
anno_df$Zscore <- scale(anno_df$confidence_score,
                         center = T,
                         scale = T)


# get the difference between IRR vs other annotator

#count the number
x <- anno_df %>% group_by(name, confidence_score) %>% tally()

#get Alex aside
irr <- x[x$name == "Alex",]

#remove Alex from main frame
x <- x %>%
      dplyr::filter(name != "Alex")

#subtract IRR vs others if their confidence match
x$difference <- x$n - irr$n[match(x$confidence_score,irr$confidence_score)]

x$n <- dplyr::coalesce(x$difference,x$n) #merge the columns
x <- x %>% dplyr::select(name, confidence_score, n) #keep the columns we need

#join the IRR back with other Annotators
x <- dplyr::bind_rows(irr, x)

#change the annotators name
anno_names <- c(
  `Alex` = "A",
  `Ean` = "E",
  `Mary` = "Ma",
  `Meghan` = "Me",
  `Tuc` = "T",
  `Wesley` = "W"
)

#graph
ggplot2::ggplot(x, aes(x=confidence_score, y=n, fill= factor(confidence_score))) + 
          geom_bar(position="dodge", stat="identity")+
          facet_grid(vars(name), labeller = as_labeller(anno_names))+
          coord_flip()+
          theme_bw()+
          theme(legend.position = "none",
                plot.title = element_text(hjust = 0.5),
                strip.background =element_rect(fill="honeydew3"),
                strip.text = element_text(colour = "black"))+
          labs(title = "IRR versus Annotator",
               x= "Confidence Score",
               y= "Changes Compare to IRR (A)",
               caption = "*NAs have been removed")+
          scale_fill_brewer(palette = "Dark2")+
          scale_y_continuous(breaks=seq(-45, 70, 5))+
          ggsave("grouped_bar_IRRvsother.png", 
                 width = 9, height = 8, units = "in")


# 
# anno_df<- anno_df[!is.na(anno_df$Zscore),]
# 
# # normalize <- function(x) {
# #   return ((x - min(x)) / (max(x) - min(x)))
# # }
# 
# # #normalize the zscore
# # anno_df$Zscore <-normalize(anno_df$Zscore)
# 
# area_z_score_min1 <- 0.1587 # from the z score table
# area_z_score_plu1 <- 0.84134 # from the z score table
# 
# shaded_area <- area_z_score_plu1 - area_z_score_min1

# #get the z score, grouped
# anno_df$z <- plyr::ddply(anno_df, .(name), summarize, z_score=scale(confidence_score))

# melt the dataframe, then get z group per confidence score for each annotator
dat <- anno_df %>% 
        dplyr::select(name, confidence_score)%>%
        tidyr::gather(variable, value, -name) %>%
        dplyr::group_by(name, variable) %>% 
        dplyr::mutate(z_score_group = (value - mean(value)) / sd(value)) %>%   
        dplyr::ungroup() %>% 
        dplyr::mutate(z_score_ungrouped = (value - mean(value)) / sd(value)) 

#form the ggridges, first for grouped zscores
ggplot2::ggplot(dat %>% mutate(group=paste(name,variable,sep="_")),
                      aes(x = z_score_group, y = name)) +
          geom_density_ridges(aes(fill = group, alpha= 0.6),
                              jittered_points = TRUE,
                              position = position_points_jitter(width = 0.05, height = 0),
                              point_shape = '|', point_size = 3, point_alpha = 1, alpha = 0.7) +
          scale_fill_manual(values = c("#00AFBB", "#E7B800", "#FC4E07",
                                       "#FF0000A0","#0000FFA0", "#8600bb")) +
          geom_vline(xintercept=0, linetype="dotted", colour = "blue") +
          theme_bw()+
          theme(legend.position = "none",
                plot.title = element_text(hjust = 0.5))+
          labs(title = "Grouped Normal Distribution Curve",
               x= "z-score (grouped)",
               y= "Annotators",
               caption = "*NAs have been removed")+
          ggsave("grouped_z_normal_curve.png", 
                 width = 9, height = 8, units = "in")

#form the ggridges, now for ungrouped zscores
ggplot2::ggplot(dat %>% mutate(group=paste(name,variable,sep="_")),
                aes(x = z_score_ungrouped, y = name)) +
          geom_density_ridges(aes(fill = group, alpha= 0.6),
                              jittered_points = TRUE,
                              position = position_points_jitter(width = 0.05, height = 0),
                              point_shape = '|', point_size = 3, point_alpha = 1, alpha = 0.7) +
          scale_fill_manual(values = c("#00AFBB", "#E7B800", "#FC4E07",
                                       "#FF0000A0","#0000FFA0", "#8600bb")) +
          geom_vline(xintercept=0, linetype="dotted", colour = "blue") +
          theme_bw()+
          theme(legend.position = "none",
                plot.title = element_text(hjust = 0.5))+
          labs(title = "Ungrouped Normal Distribution Curve",
               x= "z-score (ungrouped)",
               y= "Annotators",
               caption = "*NAs have been removed")+
          ggsave("ungrouped_z_normal_curve.png", 
                 width = 9, height = 8, units = "in")

#normal line density graph
ggplot2::ggplot(dat %>% mutate(group=paste(name,variable,sep="_")),
            aes(z_score_group, colour=group)) +
            geom_density()+
            geom_vline(xintercept=0, linetype="dotted", colour = "blue") +
            theme_bw()+
            labs(title = "Grouped Normal Distribution Curve",
                 x= "z-score (grouped)",
                 y= "Density",
                 caption = "*NAs have been removed")+
            theme(plot.title = element_text(hjust = 0.5),
                  legend.position="left")+
            scale_colour_discrete(name = "Annotators",
                                 labels = c("Alex", "Ean", "Mary",
                                            "Meghan", "Tuc", "Wesley"))

# ggplot2::ggplot(anno_df, aes(x =Zscore)) +
#                 stat_function(fun = dnorm)+
#                 stat_function(fun = dnorm, 
#                               xlim = c(-1,1),
#                               geom = "area",
#                               fill="yellow",
#                               alpha=0.5)+
#           theme_bw()+
#           labs(title = "Normal Distribution Curve",
#                x= "z-score",
#                y= "Density",
#                caption = "* only 360 rows, NAs have been removed")+
#           theme(plot.title = element_text(hjust = 0.5))+
#           annotate("text", x = -1.5, 
#                    y = 0.35, label = paste0("area: ",shaded_area), angle=35, size=5, 
#                    colour='black', face="bold")+
#           geom_segment(aes(x = -1.40, 
#                            y = 0.35, 
#                            xend = 0, 
#                            yend = 0.2), colour='royalblue', size=1,arrow = arrow(length = unit(0.5, "cm")))
#           ggsave("normal_curve.png")

readr::write_excel_csv(anno_df, "ZSCOREDannotations_fullIRR_botAttached.csv")


#EOF