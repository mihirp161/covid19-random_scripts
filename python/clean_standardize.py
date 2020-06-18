# For USF SAIL
# ~Dre, Mihir

# Script cleans and standardizes our COVID-19 JSON files
# Original JSON file is not in a standardized form
# so we will need to add an '[' at the beginning of file
# a ',' at end of line (each tweet)
# and a ']' at end of file


import sys
import re
import json
import glob


# Takes first args in .sh script
file_to_change = glob.glob("C:/Users/ThinkPad/SpyderProjects/JsonStuff/folder_with_all_jsonl/*.jsonl")

# Set save location
file_to_output_final = "C:/Users/ThinkPad/SpyderProjects/JsonStuff/Attempt to fix/" 

# Generates file name from previous filename. 
# Strips filenames to dates and add _cleaned.json string

for i, val in enumerate(file_to_change):
    file_to_output_pre = val.split('.')[0] +str("_standardized.jsonl")
    #print(file_to_output_pre)
    # Set save location
    #file_to_output_final = file_to_output_final+file_to_output_pre
    file_to_output_final = file_to_output_pre
    #print(file_to_output_final)
    
    
    output_file = open(file_to_output_final, "w")
    
    with open (val, 'r' ) as f:
        content = f.read()
    # Remove lines that are only "timestamp_ms"
    content_new = re.sub('{\"limit\":{\"track\":.*,\"timestamp_ms\":\".*\"}}\n', r'', content, flags = re.M)
    # Adds comma to end of line
    content_new2 = re.sub('\n', r',\n', content_new, flags = re.M)
    # Adds opening bracket to beginning of file
    content_new3 = '[' + content_new2
    # Adds closing bracket to end of file
    subst = "]"
    # REGEX of end of file
    regex = r",\W\Z"
    result = re.sub(regex, subst, content_new3, 0, re.MULTILINE)
    # Saves output
    output_file.write(result)
    
    #print("file location is: "+file_to_output_final)
    
#exit()
