# Covid19 - Random Scripts

This repository is home of the random data collection scripts used in Covid-19 project.       

## Contents
* R (./R)
	* **adjaceny_list_csv_maker** - Script for making csv file for gephi network visulization    
	* **eigen_joiner**- Script that reads the gephi csv file and merges with bot csv files      
	* **output_csv_fixer**- Incase of botometer crash, this will update your output.csv file, and copy the old botometers files to a new  			      directory 
	* **duplicate_check_remove**- Removes the duplicates from text files containing the twitter ids    
	* **text_file_splitter**- reads all the text files, and splits each ones based on rows you want       
	* **clean_the_cleaned_cleaned_csv**- Combines many columns we need and reduces the file size 11X folds. Note, you will need to clean the file later. However using this method, we can pick just one column rather than many columns which is a headache
	* **get_title_from_urls**- takes a csv, and replaces urls with title tags in the tweet texts.      
* python (./python)
	* **populate_bot_files** - Script for collecting Botometer scores from an output.csv *(this file contains 1 col usernames)*       
	* **username_cutter** - Reads all the csv files based on a column and returns a big csv. If you want to split that csv
			    by # of rows, you can uncomment the last lines and do it that way too.  
	* **text_file_gatherer** - Reads all the smaller USC twitter id texts files and combines them into one day. 
	
