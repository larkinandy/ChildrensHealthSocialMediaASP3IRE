### QA_Analysis.py
### Author: Andrew Larkin
### Date Created: November 22nd, 2024
### Summary: QA Analysis of tweet labelers

# import libraries
import pandas as ps
import psycopg2
import pandas.io.sql as psql

# custom driver for querying SQL database
from SQL_Class import SQL_DB

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
    locVals = list(QA_Set[questionType])
    perc = 0.0
    nLocs = 0.0
    for val in locVals:
        if locType in val:
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
        tperc,tn = calcPerfAllTimeOneLabel(labels[index],data,searchWord[index],'location',isNeg[index])
        perc[index] = tperc*100
        n[index] = tn
        df = ps.DataFrame({
        'category':labels,
        '% correct':perc,
        'n':n
    })
    return(df)

# 
def calcPerfAllTimeHealthLabel(label,data,locType,questionType,isNeg):
    screenedData = data[data['qa_type']==label]
    return(calcPercHealth(screenedData,locType,questionType,isNeg))

# main function
if __name__ == "__main__":

    # debug on local SQL database
    if(debug=='True'):
        print("enterting debug mode")
        driver = SQL_DB(dbDict,True)

    # query deployment database in the cloud
    else:
        print("entering deployment mode")0
        driver = SQL_DB(dbDict,False)

    

