import re
import csv
import glob
import os
import sys
import multiprocessing as mp
import time



#import the config csv

#import the list of regexes in the config file as an array
#import the file names associated to each regex

#the trash file where unmatched lines go
trashfile = "trash.txt"
trashpath = "h:\\proc\\"
#the path to the files you want to sort
inpath = "h:\\proc\\Input2\\"
outpath = "h:\\proc\\Output\\"
regex = []
filename = []
queues = []
watchers = []
lines_per_file = 100000

def splitfile(lines,file):
	smallfile = None
	with open(file) as bigfile:
		for lineno, line in enumerate(bigfile):
			if lineno % lines == 0:
				if smallfile:
					smallfile.close()
				small_filename = file +"_{}.txt".format(lineno + lines)
				smallfile = open(small_filename, "w")
			smallfile.write(line)
		if smallfile:
			smallfile.close()
	#delete trash
	os.remove(file)
	
def splittrash(lines,path,file):
	smallfile = None
	with open(path + file) as bigfile:
		for lineno, line in enumerate(bigfile):
			if lineno % lines == 0:
				if smallfile:
					smallfile.close()
				small_filename = inpath + "recycled_" + file +"_{}.txt".format(lineno + lines)
				smallfile = open(small_filename, "w")
			smallfile.write(line)
		if smallfile:
			smallfile.close()
	#delete trash
	os.remove(path + file)
	
def regexfunc(strinput):
	line_count =0
	matched = False
	#print(f'regexfunc queues {queues}')
	#print(f'regexfunc filenames {filename}')
	for regstring in regex:
		#print (f'trying {strinput} with {regstring}')
		#print(f'current lines {line_count}')
		match = re.findall(regstring,strinput,re.IGNORECASE)
		if match:
			#print (f'[+] Matched \"{strinput}\" with \"{regstring}\" and saving to ({line_count}) {filename[line_count]} using {queues[line_count]}')
			#with open(outpath + filename[line_count],"a") as output:
			#	output.write(strinput + "\n")
			queues[line_count].put(strinput)
			#null.put(strinput)
			matched = True
			break
		line_count += 1
		
	if not matched:
		#print (f"[+] Didnt match {strinput} and saving to trash")
		with open(trashpath + trashfile,"a") as output:
			output.write(strinput + "\n")
		matched = True

def listener(msg,filename):
	'''listens for messages on the q, writes to file. '''
	#f = open(outpath + filename, 'a', buffering=1024)
	while 1:
		g = []
		for i in range(10000):
			m = msg.get()
			if m == 'end':
				break
			g.append(m + "\n")

		#print ("[+] Writing " + m)
		
		with open(outpath + filename, 'a') as output:
			#output.write(str(m) + "\n")
			output.writelines(g)
			#g.clear()
		if m == 'end':
			break
		#f.write(str(m) + '\n')
		#f.flush()
	#f.close()

def readcsv(searchfile):
	manager = mp.Manager()
	#pool = mp.Pool(mp.cpu_count() + 2)
	with open(searchfile, "r" ) as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			#print(f'Read regex {row["regex"]} save to file {row["filename"]}.')
			#Append to the global array
			regex.append(row["regex"])
			filename.append(row["filename"])
			#create a queue
			q = manager.Queue()
			queues.append(q)
			
			#and create a listener for each line file
			#watcher = pool.apply_async(listener, (q,row["filename"]))
			watcher = mp.Process(target=listener, args=(q,row["filename"]))
			watchers.append(watcher)
			print (f'[+]setting up {watcher} in the name of {row["filename"]}')
			watcher.start()
			#watcher.join()
			print("\n")
			#pool.join()
	#setup a trash writer
	
	#print(f'readcsv queues {queues}')

def main():
	#must use Manager queue here, or will not work
	#manager = mp.Manager()
	#q = manager.Queue()  
	#pool = mp.Pool(mp.cpu_count() + 2)
	readcsv("searches.csv")
	#time.sleep(3)
	files = [file for file in glob.glob(inpath + "**/*", recursive=True)]
		
	for file in files:
		splitfile(lines_per_file,file)
	
	files = [file for file in glob.glob(inpath + "**/*", recursive=True)]
	for file in files:
		#print("reading " + file)
		#try:
		
		with open(file, 'r') as infile:
			for line in infile:
				#feed the line into the regex function
				regexfunc(line.rstrip())
		os.remove(file)	
			
		#except:
		#	print("[+]Something went wrong")
		#	pass
	#pool.close()
	proctime = time.time()
	print(f'Total regex procesomg time {proctime - start}')
	print ("[+]Finishing off")
	for q in queues:
		q.put("end")
	
	for watcher in watchers:
		
		watcher.join()
	
	try:
		splittrash(lines_per_file,trashpath,trashfile)
	except:
		pass



if __name__ == "__main__":
   start = time.time()
   main()
   end = time.time()
   print('\n')
   print(f'Total exec time {end - start}')
