
# coding: utf-8
# !/usr/bin/env python

"""Reconstruct sessions: Take a comma-delimited web server access log including ip addresses, date, time, cik (SEC central index key), accession, extension. 
Output the sessions, each session composed of an ip, the number of pages visited, the start and end time of the session, and session duration."""

import datetime
import time
from operator import itemgetter
import csv

#open input files
inactfileName = "./input/inactivity_period.txt"
with open(inactfileName, "r") as inactfile:
    inact = int(inactfile.read())

infileName = "./input/log.csv"
csvfile = open(infileName, newline='')#avoid using "with" because this closes the file 
infile = csv.DictReader(csvfile)

#initialize lists for tracking most recent sessions of each ip 
ipList = []#set of unique ips
ipIndexctr=0#initialize counter for number of ips
startList= []#most recent session start time for each ip
startSecList = []#start time in total seconds of most recent session for ip
endList = []#most recent session end time for each ip
endSecList = []#end time in total seconds for most recent session for ip
durList = []#duration of most recent session for each ip
ipoutIndex = []#index of most recent session for each ip in output 
ctList = []#most recent count of pages visited for each ip
ipnumId = []#unique index of each ip
ippageList = {}#dict of pages visited in most recent session for each ip
rowStartList = []#initialize list of starting row in dataset of most recent session for each ip
rowEndList = []#initialize list of ending row of most recent session for each ip

#initialize set of lists in whcih there will be one element for each session
ipOut = []#ip of each session
endOut = []#end time of each session formatted as strptime
endSecOut = []#end time in total seconds
startOut = []#start time of each session formatted as strptime
startSecOut=[]#start time in total seconds 
durOut = []#duration of each session in seconds
ctOut = []#count of pages visited of each session
indexOut = []#unique index of each session
rowStartOut = []#starting row of each session
rowEndOut = []#ending row of each session
indexOutctr = 0#initialize ctr for sessions
recordsRead = 0#initialize counter for rows

sessionTimeout = datetime.timedelta(seconds=inact)#initialize time after which session has timed out 

#for each row in data file infile   
for row in infile:
    
    theDate = row['date']
    theTime = row['time']
    #format date as strptime
    newRequestTime = datetime.datetime.strptime(theDate + " " + theTime, "%Y-%m-%d %H:%M:%S")
    
    ipAddr = row['ip']#get ipAddr (ip) of the row
 
    #get index of ip in ipList
    ipIndextmp = [i for i,x in enumerate(ipList) if x == ipAddr]#get ipIndextmp 
    if ipIndextmp:#make ipIndextmp an integer
        ipIndextmp = int(ipIndextmp[0])
        
    if ipIndextmp == []:#if ip has not been observed before        
        ipnumId.append(ipIndexctr)#add to list ipnumId that this is the ipIndexctr'th ip
        ipList.append(ipAddr)#add ipAddr to ipList
        endList.append(newRequestTime)#add newRequestTime as endList time for this ip
        startList.append(newRequestTime)#add newRequestTime as startList time for this ip
        durList.append(1)#initialize duration of session for this ip as 1
        startSecList.append(time.mktime(newRequestTime.timetuple()))#total seconds of start time
        endSecList.append(startSecList[ipIndexctr])#total seconds of start time is same as end time 
        pagetmp =''.join([row['cik'], row['accession'], row['extention']]) #append this page to list of pages visited in most recent session by ip                                        
        ippageList[ipAddr] = {pagetmp}#assign list of pages to ippageList for ipAddr
        ctListctr = len(ippageList[ipAddr])#assign number of pages for this session for this ip
        ctList.append(ctListctr)#ct of pages visited for each ip in most recent session
        rowStartList.append(recordsRead)#initialize first row of most recent session for ip
        rowEndList.append(recordsRead)#initialize end row of most recent session for ip
        #now make session for this ip. these values may be updated if the ip is seen again in a future row before sessionTimeout 
        ipOut.append(ipAddr)#add ipAddr to ipOut for this session
        endOut.append(newRequestTime)#assign current newRequestTime as end of session
        endSecOut.append(startSecList[ipIndexctr])#assign endSecOut for current session
        startOut.append(newRequestTime)#assign current newRequestTime as start of session
        startSecOut.append(startSecList[ipIndexctr])#assign startSecOut (start time in total seconds) using record for this ip
        durOut.append(1)#initialize duration as 1
        cttmp = ctList[ipIndexctr]#assign current count for this ip as cttmp for this session
        ctOut.append(cttmp)#assign cttmp
        indexOutctr = len(endOut)-1
        ipoutIndex.append(indexOutctr)#index of session for ip
        indexOut.append(indexOutctr)
        #ipoutIndex.append(indexOutctr)#assign current session index to record for ip
        rowStartOut.append(recordsRead)#assign current row to current session
        rowEndOut.append(recordsRead)#assign current row as end of current session
        ipIndexctr +=1#next unique ip will get this ipIndexctr
        recordsRead += 1#increment recordsRead
  
    elif newRequestTime - endList[ipnumId[ipIndextmp]]<= sessionTimeout:   
        endList[ipIndextmp] = newRequestTime#update end time for ip
        #no change to startList or startSecList
        #update total seconds of end time
        endSecList[ipIndextmp] = time.mktime(newRequestTime.timetuple())
        #update duration of current session for ip
        durList[ipIndextmp] = 1+ int(endSecList[ipIndextmp]) - int(startSecList[ipIndextmp])
        durList[ipIndextmp] = round(durList[ipIndextmp])
        pagetmp =''.join([row['cik'], row['accession'], row['extention']])                                         
        currpages = ippageList[ipAddr]
        currpages.add(pagetmp)
        currpages = set(currpages)#get unique values
        ippageList[ipAddr].update(currpages)#update list of pages
        ctListctr = len(ippageList[ipAddr])#find length (number of pages)    
        ctList[ipIndextmp]=ctListctr#assign ctList
        #do not update rowStartList of ip
        rowEndList[ipIndextmp]=recordsRead#assign current recordsRead as rowEndList of most current session for ip
        #update session
        endOut[ipoutIndex[ipIndextmp]]=newRequestTime#update end of this session (there may be no more)
        #update endSecOut for this session
        endSecOut[ipoutIndex[ipIndextmp]] = endSecList[ipIndextmp]
        #update duration for this session
        durOut[ipoutIndex[ipIndextmp]] = durList[ipIndextmp]
        ctOut[ipoutIndex[ipIndextmp]] = ctList[ipIndextmp]#update ctOut
        rowEndOut[ipoutIndex[ipIndextmp]] = recordsRead#update end of session 
        #no change to ipOut, startOut, indexOut, indexOutctr
        recordsRead+=1#increment recordsRead but do not assign it as start of session
    elif newRequestTime - endList[ipnumId[ipIndextmp]]> sessionTimeout:
        endList[ipIndextmp] = newRequestTime#reassign end time for ip
        startList[ipIndextmp] = newRequestTime#reassign start time for ip
        #update total seconds 
        startSecList[ipIndextmp]= time.mktime(newRequestTime.timetuple())#total seconds of start and end time
        endSecList[ipIndextmp] = startSecList[ipIndextmp]#start and end times are the same

        durList[ipIndextmp] = 1#initialize currnet session of ip at 1
        #reinitialize list of pages with just this page
        pagetmp =''.join([row['cik'], row['accession'], row['extention']])                                         
        ippageList[ipAddr] = {pagetmp}
        ctListctr = 1
        ctList[ipIndextmp]=ctListctr#set this back to 1
        rowStartList[ipIndextmp] = recordsRead#assign current row as start of most recent session for ip
        rowEndList[ipIndextmp] = recordsRead#assign current row as end of most recent session for ip
        
        #make new session
        ipOut.append(ipAddr)#assign ip
        startOut.append(newRequestTime)
        startSecOut.append(startSecList[ipIndextmp])#add current start seconds
        endSecOut.append(endSecList[ipIndextmp])#add current end seconds
        durOut.append(1)#initialize session as zero length
        endOut.append(newRequestTime)#assign session end time (may change)
        cttmp = ctList[ipIndextmp]#get current count for this ip (has been reset to 1)
        ctOut.append(cttmp)
        indexOutctr =len(endOut)-1 #define counter for session based on length of endOut 
        indexOut.append(indexOutctr)#index of session
        rowStartOut.append(recordsRead)#assign current recordsRead as rowStartOut of new session
        rowEndOut.append(recordsRead)#assign current recordsRead as rowEndOut of new session        
        recordsRead +=1 #increment recordsRead
        #go back to update ipoutIndex
        ipoutIndex[ipIndextmp] = indexOutctr #assign current session to record for ip        
    
durOut = [round(x) for x in durOut]

#make datetime in correct format startOut
l = len(startOut)
startstr =[]
for x in range(0,l):
    startstr.append(startOut[x].strftime("%Y-%m-%d %H:%M:%S"))
startOut = startstr

l = len(endOut)
endstr =[]
for x in range(0,l):
    endstr.append(endOut[x].strftime("%Y-%m-%d %H:%M:%S"))
endOut = endstr
##
#Rows that end before the end of the data file are not in correct order. Find the rows that end before the end of the data file, sort these by end time and then by start time. 
#Add these rows to the rows for sessions that end due to end of data file (sorted by start time)
#find the time in seconds from the final row to the end of the session
tdiff = []
for x in range(0,l):
    tdiff.append(time.mktime(newRequestTime.timetuple())-endSecOut[x] )

#find indices of tdiff greater than inact
ind = [i for i, x in enumerate(tdiff) if x > inact]
indcomplete= slice(max(ind)+1)

#find indices that were cut off by end of data
indcutoff = slice(max(ind)+1, l)

rowszip = zip(ipOut,startOut, endOut, durOut, ctOut)
rowsList = list(rowszip)#convert to list
rowsT = tuple(rowsList)

#get the rows that ended due to inactivity, rather than due to end of data 
rowsTcomplete = rowsT[indcomplete] 
#sort by endOut (column 2), then by startOut (column 1)
rowsTcomplete = sorted(rowsTcomplete, key=itemgetter(2,1))

rowsTcutoff = rowsT[indcutoff]
#sort rows of sessions cut off by end of data by start time
rowsTcutoff = sorted(rowsTcutoff , key=itemgetter(1))

#add together rowsTcomplete and rowsTcutoff
rowsT = rowsTcomplete + rowsTcutoff
##

newfilePath = "./output/sessionization.txt"
with open(newfilePath, "w") as f:
    writer = csv.writer(f)
    for row in rowsT:
        writer.writerow(row)
    
