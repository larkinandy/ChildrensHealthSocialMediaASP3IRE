### calcWeeklyPerformance.py ###
# Author: Andrew Larkin
# Date Created: May 29th, 2022
# Summary: Calculate performance metrics for students labeling tweets.
# This script connects to a cloud SQL datbase, calculates summary statistics over
# the past 14 days grouped by user id, and then writes statistics to an excel file.  
# In addition, the script randomly samples 10 labeled tweets for each student worker


# import libraries

import psycopg2 # to coonnect to SQL database
import pandas.io.sql as psql # convert sql query to pandas dataframe
import pandas as ps # write results to excel file
import datetime
import sys
from SQL_Class import SQL_DB

# load runtime arguments

debug = sys.argv[1] # true or false, whether to run in debug or deployment mode
db = sys.argv[2] # SQL param - database
user = sys.argv[3] # SQL param - username
pw = sys.argv[4] # SQL param - password
host = sys.argv[5] # SQL param - host
tempFolder = sys.argv[6] # where intermediate results are written to.  Not a stable long term storage folder
outputFolder = sys.argv[7] # where output results are written to.  
startDate = sys.argv[8] # start date for the time of interest, inclusive
endDate = sys.argv[9] # end date for the time of interest, inclusive

# global constants
dbDict = {
    'host':host,
    'db':db,
    'user':user,
    'pw':pw
}

####### HELPER FUNCTIONS ######

# calculate whether a tweet was labeled using a generic, often overused 'unsure' category
# INPUTS:
#    record (pandas dataframe) - single row containing the label
# OUTPUTS:
#    nUnsure (int) - number of times the tweet was labeled with a generic category
#    nNo (int) - number of times the tweets was labeled with a negative classification (i.e. easier/faster to label)
def calcUnsureNoOneRecord(record):
    nUnsure,nNo = 0,0
    if(record['location_cat'][0]=='No location/unsure' or record['location'][0] in (["Other","Unsure (try to guess, only use if really unsure)"])):
        nUnsure +=1
    if(record['age'][0] == 'Unsure'):
        nUnsure +=1
    if(record['is_child']==False):
        nNo +=1
    if(record['is_health']==False):
        nNo +=1
    if(record['health_impact'][0]=="No impact"):
        nUnsure +=1
    return(nUnsure,nNo)

# calculate the number of unsure and negative labels for a given worker
# INPUTS:
#    labeledRecords (pandas dataframe) - records labeled by the worker
# OUTPUTS:
#    nRecords (int) - number of labeled records
#    percUnsure (float) - percentage of records labeled using a generic unsure category
#    percNo (float) - percentage of tweets with a negative classification label (faster to label)
def calcUnsureOneUser(labeledRecords):
    nRecords = labeledRecords.count()[0]
    nUnsure,nNo = 0,0
    for recordIndex in range(0,nRecords):
        curRecord = labeledRecords.iloc[recordIndex]
        tempVals = calcUnsureNoOneRecord(curRecord)
        nUnsure += tempVals[0]
        nNo += tempVals[1]
    percUnsure = int(nUnsure/(1.0*nRecords*3)*100)
    percNo = int(nNo/(1.0*nRecords*2)*100)
    return(nRecords,percUnsure,percNo)

# get number of tweets coded by each worker for a single day
# INPUTS:
#    driver (SQL_DB) - custom class object that queries SQL database
#    startStamp (timestamp) - startDate, inclusive
#    tableName (string) - table to query
# OUTPUTS:
#    pandas dataframe containing number of tweets coded by each worker
def getTweetsByDayAllUsers(driver,startStamp,tableName):
    nextDay = startStamp + datetime.timedelta(days=1)
    return(driver.getUserTweetsInTime(startStamp.day,startStamp.month,nextDay.day,nextDay.month,nextDay.year,tableName))
    
# calculate performance for all workers during a specific time interval
# INPUTS:
#    driver (SQL_DB) - custom class object that queries SQL database
#    startDate (timestamp) - startDate,inclusive
#    endDate (timestamp) - endDate, inclusive
#    tableName (str) - name of table to query
# OUTPUTS:
#    dataframe containing the performance metrics for each worker
def calcPerformanceAllUsers(driver,startDate,endDate,tableName):
    df = {}

    # get users who coded at least 1 tweet during the given time interval
    df['user_id'] = driver.getUserIdsInTime(startDate.day,startDate.month,endDate.day,endDate.month,2023,tableName)
    unsureArr,noArr,nTweets = [],[],[]

    # for each worker calculate performance metrics
    for user in df['user_id']:

        # get tweets labeled by the worker
        labeledRecords = driver.getTweetsOneUser(user,startDate.day,startDate.month,endDate.day,endDate.month,2023,tableName)

        # calculate percent unsure and negative labeled
        perf = calcUnsureOneUser(labeledRecords)
        nTweets.append(perf[0])
        unsureArr.append(perf[1])
        noArr.append(perf[2])
    df['n_tweets'] = nTweets
    df['perc_unsure'] = unsureArr
    df['perc_no'] = noArr

    # calculate number of tweets labeled during each day, to see if worker productivity matches timesheet
    date = startDate
    while((endDate-date).days>=0):
        nextDay = date+datetime.timedelta(days=1)
        dailyVals = getTweetsByDayAllUsers(date,nextDay)
        tempArr = []
        for user in df['user_id']:
            if(user in list(dailyVals['user_id'])):
                curRecord = dailyVals[dailyVals['user_id']==user]
                tempArr.append(list(curRecord['n_tweets'])[0])
            else:
                tempArr.append(0)
        df[str(date.month) + "-" + str(date.day)] = tempArr
        date = nextDay

    # return user performance metrics
    df2 = ps.DataFrame(df)
    return(df2)

# get 10 randomly sampled labeled tweets for all workers
# INPUTS:
#    driver (SQL_DB) - custom class object that queries SQL database
#    outputFilepath (str) - absolute filepath where randomly sampled tweets will be stored as a .csv
#    startDate (timestamp) - start of the random sample time window (inclusive)
#    endDate (timestamp) - end of the random sample time window (inclusive)
#    tableName (str) - table to query
def getUserSamples(driver,outputFilepath,startDate,endDate,tableName):
    uniqueUsers = driver.getUserIdsInTime(startDate.day,startDate.month,endDate.day,endDate.month,endDate.year,tableName)
    print("found %i users who labeled records in the past 2 weeks" %(len(uniqueUsers)))
    if(len(uniqueUsers)==0):
        return
    df = driver.selectRandomForUser(uniqueUsers[0],tableName)
    for user in uniqueUsers[1:]:
        tempdf = driver.selectRandomForUser(user,tableName)
        df = df.append(tempdf)
    df.to_csv(outputFilepath,index=False)

# combine multiple csv files into an Excel workbook
# INPUTS:
#    outputFilepath (str) - absolute filepath where Excel file will be stored
#    inputCSVs (str array) - absolute filepaths to csv files used to create Excel workbook. One CSV for each page
def writeResultsToExcel(outputFilepath,inputCSVs):
    writer = ps.ExcelWriter(outputFilepath)
    names = ['weekly performance','random sample']
    index= 0
    print("saving student performance to %s" %(outputFilepath))
    for csvfilename in inputCSVs:
            df = ps.read_csv(csvfilename)
            df.to_excel(writer,names[index])
            index+=1
    writer.save()


####### MAIN FUNCTION ######

if __name__ == "__main__":

    # debug on local SQL database
    if(debug=='True'):
        print("enterting debug mode")
        tableName = "twitter_labels_test"
        driver = SQL_DB(dbDict,True)

    # query deployment database in the cloud
    else:
        print("entering deployment mode")
        tableName = "twitter_labels"
        driver = SQL_DB(dbDict,False)

    endDate = datetime.datetime(endDate)
    startDate = datetime.datetime(startDate)
    #startDate = endDate-datetime.timedelta(days=31)
    perfCSV = tempFolder + "overallPerformance.csv"
    sampleCSV = tempFolder + "testRandomSample.csv"
    performance = calcPerformanceAllUsers(driver,startDate,endDate,tableName)
    performance.to_csv(perfCSV,index=False)
    getUserSamples(driver,sampleCSV,startDate,endDate,tableName)
    weeklyExcel = outputFolder + "student_performance_" + str(endDate.month) + '-' + str(endDate.day) + '-' + str(endDate.year) +  ".xlsx"
    writeResultsToExcel(weeklyExcel,[perfCSV,sampleCSV])
    
# end of calcWeeklyPeformance.py 