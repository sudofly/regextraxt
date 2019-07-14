import re
import csv
import glob
import os
import sys
import multiprocessing as mp
import time
import random

# import the config csv

# import the list of regexes in the config file as an array
# import the file names associated to each regex

# the trash file where unmatched lines go
trashfile = "trash.txt"
trashpath = "e:\\proc\\"
# the path to the files you want to sort
inpath = "e:\\proc\\Input\\"
outpath = "e:\\proc\\Output\\"
regex = []
filename = []
queues = []
watchers = []
lines_per_file = 100000


def splitfile(file):
    smallfile = None
    with open(file, errors='ignore') as bigfile:
        for lineno, line in enumerate(bigfile):
            if lineno % lines_per_file == 0:
                if smallfile:
                    smallfile.close()
                small_filename = file + "." + str(random.randint(1, 1000)) + ".{}.txt".format(lineno + lines_per_file)
                smallfile = open(small_filename, "w")
            smallfile.write(line)
        if smallfile:
            smallfile.close()
    # delete trash
    os.remove(file)


def splittrash(lines, path, file):
    smallfile = None
    with open(path + file) as bigfile:
        for lineno, line in enumerate(bigfile):
            if lineno % lines == 0:
                if smallfile:
                    smallfile.close()
                small_filename = inpath + "recycled_" + file + "_{}.txt".format(lineno + lines)
                smallfile = open(small_filename, "w")
            smallfile.write(line)
        if smallfile:
            smallfile.close()
    # delete trash
    os.remove(path + file)


def regexfunc(strinput):
    line_count = 0
    matched = False
    # print(f'regexfunc queues {queues}')
    # print(f'regexfunc filenames {filename}')
    for regstring in regex:
        # print (f'trying {strinput} with {regstring}')
        # print(f'current lines {line_count}')
        match = re.findall(regstring, strinput, re.IGNORECASE)
        if match:
            # print (f'[+] Matched \"{strinput}\" with \"{regstring}\" and saving to ({line_count}) {filename[line_count]} using {queues[line_count]}')
            # with open(outpath + filename[line_count],"a") as output:
            #	output.write(strinput + "\n")
            queues[line_count].put(strinput)
            # null.put(strinput)
            matched = True
            break
        line_count += 1

    if not matched:
        # print (f"[+] Didnt match {strinput} and saving to trash")
        with open(trashpath + trashfile, "a") as output:
            output.write(strinput + "\n")
        matched = True


def listener(msg, filename):
    '''listens for messages on the q, writes to file. '''
    # f = open(outpath + filename, 'a', buffering=1024)
    while 1:
        g = []
        for i in range(10000):
            m = msg.get()
            if m == 'end':
                break
            g.append(m + "\n")

        with open(outpath + filename, 'a') as output:
            output.writelines(g)
        if m == 'end':
            break


def readcsv(searchfile):
    manager = mp.Manager()
    # pool = mp.Pool(mp.cpu_count() + 2)
    with open(searchfile, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for row in csv_reader:
            # print(f'Read regex {row["regex"]} save to file {row["filename"]}.')
            # Append to the global array
            regex.append(row["regex"])
            filename.append(row["filename"])
            # create a queue
            q = manager.Queue()
            queues.append(q)

            # and create a listener for each line file
            # watcher = pool.apply_async(listener, (q,row["filename"]))
            watcher = mp.Process(target=listener, args=(q, row["filename"]))
            watchers.append(watcher)
            print(f'[+]setting up {watcher} in the name of {row["filename"]}')
            watcher.start()
            print("\n")


# setup a trash writer

def main():
    # must use Manager queue here, or will not work
    pool = mp.Pool(mp.cpu_count() + 2)

    # files = [file for file in glob.glob(inpath + "**/*", recursive=True) if not os.path.isdir(file)]
    files = [file for file in glob.glob(inpath + "**/*", recursive=True) if not os.path.isdir(file) if
             os.path.getsize(file) > 5000000]
    # for file in files:
    # print (files)
    pool.map(splitfile, files)
    pool.close()
    readcsv("searches.csv")
    files = [file for file in glob.glob(inpath + "**/*.*", recursive=True) if not os.path.isdir(file)]
    for file in files:
        # print("reading " + file)
        try:

            with open(file, 'r') as infile:
                for line in infile:
                    # feed the line into the regex function
                    regexfunc(line.rstrip())
                # spawnedregex = pool.apply_async(regexfunc(line.rstrip()))
            os.remove(file)

        except:
            print("[+]Something went wrong, moving on to the next file")
            pass

    proctime = time.time()
    print(f'Total regex processing time {proctime - start}')
    print("[+]Finishing off")
    for q in queues:
        q.put("end")

    for watcher in watchers:
        watcher.join()

    try:
        splittrash(lines_per_file, trashpath, trashfile)
    except:
        pass


if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print('\n')
    print(f'Total exec time {end - start}')
