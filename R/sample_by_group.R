# Library -----------------------------------------------------------------
library(openxlsx)

# Task --------------------------------------------------------------------

# 1. Import an excel file.
# 2. Remove irrelevant columns and duplicate data.
# 3. Get a percentage of randomly sampled rows, grouped by a categorical column.
# 4. Export the result as an excel file.


# 1. Load Data ------------------------------------------------------------

# Use the openxlsx package to import an Excel file.
df2 <- read.xlsx("DGRL15.5.merged.xlsx")

# 2. Clean Data -----------------------------------------------------------


# Keep relevant columns
df <- df[, c("year", "document title", "Query")]

# Remove duplicates
df <- df[!duplicated(df$`document title`), ]



# 3.1 Create Function -----------------------------------------------------

# Create a function.
# This samples a percentage of rows from each category in the dataframe.
# The same functionality is possible through a group_by() with dplyr.

sample_by_group <- function(dataset, column, percent){
  
  # Nested function! Get a random sample of rows.
  samples <- function(dataset){
    
    # How many samples?
    length <- nrow(dataset)
    # How large of a sample size?
    sample_size <- ceiling(percent*length)
    # What are the indices?
    indices <- sample(1:length,
                      size= sample_size,
                      replace = FALSE)
    # Get the samples
    sampled_dataset <- dataset[indices, ]
    # Return the samples
    return(sampled_dataset)
  }
    
  # Get a random sample of rows after grouping the dataset by a column.
  # This returns a list.
  grouped_samples_list <- by(dataset, 
          dataset[, column], 
          samples)
  
  # Turn the list into a dataframe.
  grouped_samples <- do.call(what = 'rbind', 
                             args = grouped_samples_list)
  
  # Return the grouped samples dataframe.
  return(grouped_samples)
  
}


# 3.2. Sample By Group ----------------------------------------------------

# Run the function, based on needs. Percentage and column are adjustable.
result <- sample_by_group(df, "document title", .10)

# Save --------------------------------------------------------------------

# Use the openxlsx package to export the result.
write.xlsx(result, "DGRL15.5_samples.xlsx")


