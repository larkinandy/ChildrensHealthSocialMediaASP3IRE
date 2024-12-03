### QA_Analysis.py
### Author: Andrew Larkin
### Date Created: November 22nd, 2024
### Summary: QA Analysis of tweet labelers

# import libraries
import pandas as ps
import psycopg2
import pandas.io.sql as psql
import sys

# custom driver for querying SQL database
from SQL_Class import SQL_DB
from mySecrets import secrets

# load runtime arguments
debug = sys.argv[1] # true or false, whether to run in debug or deployment mode
db = sys.argv[2] # SQL param - database
user = sys.argv[3] # SQL param - username
pw = sys.argv[4] # SQL param - password
host = sys.argv[5] # SQL param - host

dbDict = {
    'host':host,
    'db':db,
    'user':user,
    'pw':pw
}

# for one subset of label and classification (positive or negative),
# calculate percent of coded QA tweets whose worker labels correctly match the QA labels
# INPUTS:
#    QA_Set (pandas dataframe) - set of coded QA tweets to analyze
#    label (str) - deep learning model label (e.g. baby, infant, house, etc)
#    category (str) - deep learning model (place,child,or health)
#    isNeg (Boolean) - is the QA classification positive or negative
# OUTPUTS:
#    perc (float) - percent of tweets that were correctly coded for the label/classification combo
#    (int) - number of tweets that were correctly coded for the label/classification combo
def calcPercCorrect(QA_Set,label,category,isNeg=False):
    locVals = list(QA_Set[category])
    perc = 0.0
    nLocs = 0.0
    for val in locVals:
        if label in val:
            nLocs+=1
        perc = nLocs/len(locVals)
        if(isNeg==True):
            perc = 1-perc
    return([perc,len(locVals)])

# for all labels in the 'place' category, calculate percent of coded QA tweets whose worker labels
# correctly match the QA labels
def calcLoc(data):

    # QA codes
    labels = ["school_neg","school_pos","home_neg","home_pos","daycare_neg","daycare_pos","park_neg","park_pos"]

    # whether QA codes are positive or negative classifications 
    isNeg = [True,False,True,False,True,False,True,False]

    # labels
    searchWord = ["School","School","home","home","daycare","daycare","Park","Park"]
    
    # create arrays to store the percent correct for each label/classification combo
    perc,n = [0.0 for x in labels],[0 for x in labels]

    # for each QA code type, calculate the percent tweets that were correctly labeled by workers
    # and store in a pandas dataframe
    for index in range(len(labels)):
        tperc,tn = calcPercCorrect(data,labels[index],searchWord[index],isNeg[index])
        perc[index] = tperc*100
        n[index] = tn
        df = ps.DataFrame({
        'category':labels,
        '% correct':perc,
        'n':n
    })
    return(df)

# for all labels in the 'health' category, calculate percent of coded QA tweets whose worker labels
# correctly match the QA labels
def calcHealth(data):

    # QA codes
    labels = ["emot_neg","emot_pos","phys_neg","phys_pos","cog_neg","cog_pos"]

    # whether QA codes are positive or negative classifications 
    isNeg = [True,False,True,False,True,False,True,False]

    # labels
    searchWord = ["emot","emot","phys","phys","cog","cog"]
    
    # create arrays to store the percent correct for each label/classification combo
    perc,n = [0.0 for x in labels],[0 for x in labels]

    # for each QA code type, calculate the percent tweets that were correctly labeled by workers
    # and store in a pandas dataframe
    for index in range(len(labels)):
        tperc,tn = calcPercCorrect(data,labels[index],searchWord[index],isNeg[index])
        perc[index] = tperc*100
        n[index] = tn
        df = ps.DataFrame({
        'category':labels,
        '% correct':perc,
        'n':n
    })
    return(df)

# for all labels in the 'age' category, calculate percent of coded QA tweets whose worker labels
# correctly match the QA labels
def calcAge(data):

    # QA codes
    labels = ["baby_neg","baby_pos","toddler_neg","toddler_pos","elem_neg","elem_pos",'middle_neg','middle_pos','high_neg','high_pos']

    # whether QA codes are positive or negative classifications 
    isNeg = [True,False,True,False,True,False,True,False]

    # labels
    searchWord = ["baby","baby","toddler","toddler","elem","elem",'middle','middle','high','high']
    
    # create arrays to store the percent correct for each label/classification combo
    perc,n = [0.0 for x in labels],[0 for x in labels]

    # for each QA code type, calculate the percent tweets that were correctly labeled by workers
    # and store in a pandas dataframe
    for index in range(len(labels)):
        tperc,tn = calcPercCorrect(data,labels[index],searchWord[index],isNeg[index])
        perc[index] = tperc*100
        n[index] = tn
        df = ps.DataFrame({
        'category':labels,
        '% correct':perc,
        'n':n
    })
    return(df)


# main function
if __name__ == "__main__":

    # debug on local SQL database
    if(debug=='True'):
        print("enterting debug mode")
        driver = SQL_DB(dbDict,True)

    # query deployment database in the cloud
    else:
        print("entering deployment mode")
        driver = SQL_DB(dbDict,False)

    # get all QA coded records
    allRecords = driver.getQATweets()

    # subset QA records to age coded records and claculate QA performance
    childRecords = allRecords[allRecords['category']=='age']
    childDF = calcAge(childRecords)

    # subset QA records to place coded records and calculate QA performance
    placeRecords = allRecords[allRecords['category']=='place']
    placeDF = calcLoc(placeRecords)

    # subset health records and calculate QA performance
    healthRecords = allRecords[allRecords['category']=='health']
    healthDF = calcHealth()

    # save QA performance to disk
    combinedDF = ps.concat([childDF,placeDF,healthDF])
    combinedDF.to_csv(secrets['QA_FILEPATH'],index=False)


    
# end of QA_Analysis.py