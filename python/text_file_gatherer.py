#For USF SAIL
# ~Mihir Patel
import os
import re
import glob

# Put this file in the firectory of texts files, meaning this script will house where your files are
# file path for somereason is not working here.

#Regex that keeps makes the buffer for number parts
date_re = re.compile(r'^coronavirus-tweet-id-(\d{4}-\d{2}-\d{2})-\d{2}\.txt$')
prev_date = None #to track previous dates

new_list = [] #List that holds text files

#It goes like: Read all the date files once, turn them into one giant output file then repeat
for file in glob.glob("coronavirus-tweet-id-*.txt"):
    m = date_re.search(file)
    if m:
        date = m.group(1)
        print(f'Working on day {date} ...')
        with open(file) as fin:
            with open(f'output-{date}.txt', 'a') as fout:
                fout.write(fin.read())

#EOF