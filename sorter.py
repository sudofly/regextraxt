import re
import csv
import glob
import os

#import the config csv

#import the list of regexes in the config file as an array
#import the file names associated to each regex

#the trash file where unmatched lines go
trashfile = "trash.txt"
#the path to the files you want to sort
inpath = "c:\\Sorter\\Combos\\"
outpath = "c:\\Sorter\Output\\"
regex = []
filename = []
lines_per_file = 10000000

def splitfile(lines,file):
	smallfile = None
	with open(file) as bigfile:
		for lineno, line in enumerate(bigfile):
			if lineno % lines == 0:
				if smallfile:
					smallfile.close()
				small_filename = inpath + "recycled_trash_{}.txt".format(lineno + lines)
				print (outpath +"Small_file")
				smallfile = open(small_filename, "w")
			smallfile.write(line)
		if smallfile:
			smallfile.close()
	#delete trash
	os.remove(file)
		
def regexfunc(strinput):
    line_count =0
    matched = False
    for regstring in regex:
        match = re.findall(regstring,strinput,re.IGNORECASE)
        if match:
            #print (f'[+] Matched \"{strinput}\" with \"{regstring}\" and saving to ({line_count}) {filename[line_count]}')
            with open(outpath + filename[line_count],"a") as output:
                output.write(strinput + "\n")
            matched = True
            break
        line_count += 1
        
    if not matched:
        #print (f"[+] Didnt match {strinput} and saving to trash")
        with open(trashfile,"a") as output:
            output.write(strinput + "\n")
        matched = True

#processing the csv file and extracting the regex's

with open("searches.csv", "r" ) as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    for row in csv_reader:
        print(f'Read regex {row["regex"]} save to file {row["filename"]}.\n')
        regex.append(row["regex"])
        filename.append(row["filename"])
    print("\n")


#look for files in the path variable and open each for reading

files = [file for file in glob.glob(inpath + "**/*", recursive=True)]


for file in files:
    print(file)
    try:
        with open(file, 'r') as infile:
            for line in infile:
            #feed the line into the regex function
                regexfunc(line.rstrip())
            #finished processing so delete the file
        os.remove(file)
    except:
        pass

#cleanup the trash
splitfile(lines_per_file,trashfile)