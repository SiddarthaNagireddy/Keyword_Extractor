'''
Created on 27-Mar-2015
@author: siddarth
'''

import rake
import multiprocessing as mp
import time
import os
import csv

filename = 'C:/Users/Siddartha.Reddy/Desktop/testfiles/contacts_segments_title000'
outName = 'Job_Title_Keywords'
BYTES_PER_MB = 1048576

start = time.time()
def elapsed():
    return time.time() - start

''' Test Function to execute brute force '''
def regular_exe():
    start = time.time()
    count = 0
    stopwordlist = 'smartstoplist.txt'
    with open(filename,'r') as fd:
        line = fd.read()
        print(line)
    end = time.time()
    print(count)
    print("TOTAL TIME ",(end-start))

def getJobTitle(line):
    title = line.split('\t')
    try:
        job = title[2]
    except:
        job = ''
    if '/' in job:
        job = job.replace('/',' and ')
    elif '&' in job:
        job = job.replace('&',' and ') 
    return job

''' Worker function used to call rake '''
def fileparser_worker(filename, start, end, c):
    outFileName = outName + str(c) 
    outFile = open('C:/Users/Siddartha.Reddy/Desktop/testfiles/'+outFileName,'w')
    outFileBuffer = []
    with open(filename) as inFile:
        inFile.seek(start)
        for line in inFile.read(end-start).split('\n'):
            job = getJobTitle(line)
            try:
                keywords = rake.execute_rake(job)
            except:
                print('Exception occured!')
            outFileBuffer.append(line+'\t'+str(keywords)+'\n')
            if len(outFileBuffer) == 10000000:
                outFile.writelines([x for x in outFileBuffer])
                outFileBuffer = []
        outFile.writelines([x for x in outFileBuffer])
        outFile.close()
    
if __name__ == '__main__':
    start = time.time()
    chunk_start = 0
    
    chunk_size =  512 * BYTES_PER_MB
    chunk_end = 512 * BYTES_PER_MB
    filesize = os.path.getsize(filename)
    
    print '\n%.3fs: file is %s large' % (elapsed(), filesize)
    c = 0
    pause = 0
    iterations = (filesize / chunk_size) + 1
    ''' Chunk iterations based on linecount ''' 
    print(iterations)
    with open(filename) as inFile:
        while c < iterations:
            if chunk_start + chunk_size > filesize:
                chunk_end = filesize
            else:
                chunk_end = chunk_start + chunk_size
 
            print("Start chunk",chunk_start)
            ''' Initializing Processes '''
            proc = mp.Process(target=fileparser_worker, args = (filename,chunk_start,chunk_end, c) )
            proc.start()
            pause += 1
            print("End chunk",chunk_end)
            chunk_start = chunk_end
            c += 1
            
            ''' Pausing Processes so that at a time there are atmost 4 processes running '''
            if pause == 4:
                proc.join() # Waits for processes to close to avoid zombie processes
                pause = 0
            
    proc.join()
    print("TOTAL TIME TAKEN",elapsed())

# regular_exe()  