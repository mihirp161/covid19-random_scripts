# Covid19 - Random Scripts

This repository contains random scripts. You can pick them up as you find them useful. For question, refer to the individual listed on the top.            

## Contents
* R (./R)
	* **adjaceny_list_csv_maker** - Script for making csv file for gephi network visulization.        
	* **eigen_joiner**- Script that reads the gephi csv file and merges with bot csv files.         
	* **output_csv_fixer**- Incase of botometer crash, this will update your output.csv file, and copy the old botometers files to a new  			      directory.      
	* **duplicate_check_remove**- Removes the duplicates from text files containing the twitter ids.    
	* **text_file_splitter**- reads all the text files, and splits each ones based on rows you want.       
	* **clean_the_cleaned_cleaned_csv**- Combines many columns we need and reduces the file size 11X folds. Note, you will need to clean the file later. However using this method, we can pick just one column rather than many columns which is a headache.
	* **get_title_from_urls**- takes a csv, and replaces urls with title tags in the tweet texts.        
	* **big-data-splitter-csv**- takes a giant csv and makes many child csv based on the number of rows given.       
	* **singly_outputcsv_maker_randSampled**- takes all csvs from USC data, and creates distributable output csvs for botometer process.      
	* **keep_processed_bots_only**- fixes the USC's output_YOURNAME.csv file by removing the names which we've processed already.    
	* **file_renamer**- renames all the files so we all follow same naming convention.      
	* **specific_column_csv_writer**- reads a csv, you can select columns based on one's need then writes with those columns.
	* **Rclean_json_step2**- R script to replace *clean_json_step2.py*, because in some linux machine, Pandas is behaving strangely like shifting columns. If you're on windows, you can use this or *clean_json_step2.py.*
	* **production_parellel**- R script to replace *clean_json_step1.py*, when the SLURM is being weird to us.
* python (./python)
	* **populate_bot_files** - Script for collecting Botometer scores from an output.csv *(this file contains 1 col usernames)*       
	* **username_cutter** - Reads all the csv files based on a column and returns a big csv. If you want to split that csv
			    by # of rows, you can uncomment the last lines and do it that way too.  
	* **text_file_gatherer** - Reads all the smaller USC twitter id texts files and combines them into one day.    
	* **clean_json_step1** - Re-dumps the jsonl files to remove extra EOFs, Reads a jsonl (or many jsonls through merging) from hydrator and remakes a twitter csv in utf-8 BOM format to preserve emojis. Use this on Hydrator files, if you are cleaning json files from twitter then know that full_text objects in script are actually only text in other json files from like Twitter.        
	* **clean_json_step2** - Reads bunch of USC data and botscores we got, and make a 100K csv file with URL being replaced with the url's <title> tag.        
	* **re_make_jsonl** - only works on jsonlines files. Remove all extra EOF from the files, and formats so every line has '\n' delimiter. 
	* **500_rows_maker** - filters a dataframe based on queries.
	* **replace_url_stripped** - A stripped down version of clean_json_step2.py, that only removes only removes the URL with <title> tag.
