### filterTrainingRecords.py
### Author: Andrew Larkin
### Date Created: November 22nd, 2024
### Summary: Script for reading labeled training records in an sql database, 
###          filtering records by worker QA cutoffs, joining labels with twitter records, 
#            and saving filtered records as a csv

# import libraries
import pandas as ps
import psycopg2
import pandas.io.sql as psql
import numpy as np
from SQL_Class import SQL_DB

# global constants
dbDict = {
    'host':'insert host here',
    'db':'insert db here',
    'user':'insert user here',
    'pw':'insert pw here'
}

# only use labeled records from verified workers
user_ids = [
    'reconcile_tune','slam_cooperative','implication_attic','rent_development','function_curriculum',
    'team_rocket','exchange_expectta','slam_cooperative  ','implication_attic ','exchange_expectn ',
    'productive_cinema','productive-cinema','function_curriculum ','exchange_expect ','skate_establish',
    'integrity_prospect','integrity_prospecti','inspector_rocket','exchange_expect1','Integrity_prospect',
    'exchange_expect','implication_attic'
    ]

# relationship between child age group text and index in a multiclassification array
childCodingDict = {
    '0 to less than 1 year (baby/infant)':0,
    '1 to 4 years (toddler/pre-school)':1,
    '5 to 10 years (elementary school)':2,
    '11 to 13 years (middle school)':3,
    '14 to 17 years (high school)':4,
    'School age (no specific school type)':5,
    'No specific age (children general)':5,
    'Unsure':5
}

# relationship between whether a tweet mentions a child and binary classification
ageCodingDict = {
    True:1,
    False:0
}

# relationship between place type and index in a multiclassification array
placeCodingDict = {
    'Childcare/daycare':0,
    'Park/playground/child sports center':1,
    'A home':2,
    'School':3,
    'Neigborhood (but not on home property, etc)':4,
    'Other':5,
    'Unsure':5,
    'No location':6
}

# relationship between place setting and index in a multiclassification array
inDoorOutdoor = {
    'Indoor location':7,
    'Outdoor location':8,
    'No location/unsure':9
}

# relationship between health impact and index in a multiclassification array
healthImpact = {
    'Negative impact':1,
    'Positive impact':2,
    'No impact':3
}

# relationship between health impact type and index in a multiclassification array
healthType = {
    'Cognitive health':4,
    'Emotional/social health':5,
    'Physical health':6
}

###### HELPER FUNCTIONS ######

# load records from SQL database
# INPUTS:
#    driver (SQL_DB class) - instance of the SQL_DB class for querying custom SQL database
# OUTPUTS:
#    origRecords (pandas dataframe) - original set of tweets which were labeled for all categories
#    childTweets (pandas dataframe) - set of tweets labeled just for child categories
#    placeTweets (pandas dataframe) - set of tweets labeled just for place categories
#    healthTweets (pandas dataframe) - set of tweets labeled just for health categories
#    labels (pandas dataframe) - labels created by workers, not yet joined to raw tweet data
def getRecordsFromSQL(driver):

    # get records from dataset
    origRecords = driver.getOrigTwitterRecords()
    childTweets = driver.getChildTwitterRecords()
    placeTweets = driver.getPlaceTwitterRecords()
    healthTweets = driver.getHealthTwitterRecords()
    labels = driver.getTweetLabels()

    # print number of records returned from queries
    print("labeled records results:")
    print("original: %i \n child : %i\n place: %i\n health: %i\n labels: %i" %(
        origRecords.count().iloc[0],
        childTweets.count().iloc[0],
        placeTweets.count().iloc[0],
        healthTweets.count().iloc[0],
        labels.count().iloc[0],
        ))
    return([origRecords,childTweets,placeTweets,healthTweets,labels])

# remove records that were for exercises to train workers how to label tweets
# INPUTS:
#    data (pandas dataframe) - entire records
# OUTPUTS:
#    subset of input dataset, with training examples removed
def filterDataset(data):
    data = data[data.user_id.isin(user_ids)]
    data = data[data['keyword']!='training']
    return(data)

# combine tweets with labels and remoe records that were labeled outside of the designated switch in labeling design
# (labeling all categories vs. labeling just a single category)
# INPUTS:
#    dataset (pandas dataframe) - tweets without labels attached
#    labels (pandas dataframe) - labels to attach to tweets
#    dataName (str) - category of data type (child, place, or health)
#    beforeChange (Boolean) - whether tweets correspond to before or after the designated switch in labeling design
# OUTPUTS:
#    labeledDataset (pandas dataframe) - tweets joined with labels
def mergeAndFilterDataset(dataset,labels,dataName,beforeChange=True):

    # merge twitter records with labels and remove records used to train workers
    labeledDataset = dataset.merge(labels,how='inner',on='img_id')
    labeledDataset = filterDataset(labeledDataset)

    # remove records that might have been accidentally coded using the wrong labeling design
    if(beforeChange):
        labeledDataset.sort_values(by='time_submitted', ascending=False,inplace=True)
        labeledDataset = labeledDataset[(labeledDataset['time_submitted']< '2022-09-15 00:00:00')]
    else:
        labeledDataset = labeledDataset[(labeledDataset['time_submitted']>= '2022-09-15 00:00:00')]
        labeledDataset.sort_values(by='time_submitted',ascending=True,inplace=True)
    print("number of records in %s: %i" %(dataName,labeledDataset.count().iloc[0]))
    return(labeledDataset)

# for tweets that were labeled multiple times, only keep one of the labels. Remove 
# INPUTS:
#    inData (pandas dataframe) - contains labeled tweets
#    newCats (str array) - labels to keep
# OUTPUTS:
#    inDataRed2 (pandas dataframe) - dataset with duplicates removed
def removeRedundant(inData,newCats):
    # reduce dataset to only the columns that are relevant for the current category (child,health,place)
    columnsToKeep = newCats + ['img_id','text','keyword','category','img_http']
    inDataRed = inData[columnsToKeep]

    # only keep one label for each tweet
    inDataRed.drop_duplicates(subset=['img_id'],keep='first',inplace=True)
    return(inDataRed)

# for all training label categories, join twitter records to labels
# INPUTS:
#    origRecords (pandas dataframe) - original set of tweets which were labeled for all categories
#    childRecords (pandas dataframe) - set of tweets just for child categories
#    placeRecords (pandas dataframe) - set of tweets just for place categories
#    healthRecords (pandas dataframe) - set of tweets just for health categories
#    labels (pandas dataframe) - labels created by workers, not yet joined to raw tweet data
# OUTPUTS:
#    origLabeled (pandas dataframe) - original set of tweets joined to labels
#    childLabeled (pandas dataframe) - set of tweets just for child categories joined to labels
#    placeLabeled (pandas dataframe) - set of tweets just for place categories joined to labels
#    healthLabeled (pandas dataframe) - set of tweets just for health categories joined to labels
def joinLabelsToRecords(origRecords,childRecords,placeRecords,healthRecords,labels):
    origLabeled = mergeAndFilterDataset(origRecords,labels,'orig records')
    childLabeled = mergeAndFilterDataset(childRecords,labels,'child records')
    placeLabeled = mergeAndFilterDataset(placeRecords,labels,'place records')
    healthLabeled = mergeAndFilterDataset(healthRecords,labels,'health records')
    return([origLabeled,childLabeled,placeLabeled,healthLabeled])

# for each label category (child,place,health), joined tweets that were labeled before and after the designed label change
# INPUTS:
#    origRecords (pandas dataframe) - original set of tweets which were labeled for all categories
#    childRecords (pandas dataframe) - set of tweets just for child categories
#    placeRecords (pandas dataframe) - set of tweets just for place categories
#    healthRecords (pandas dataframe) - set of tweets just for health categories
# OUTPUTS:
#    joinedTweets (list of pandas dataframes)
#       - index 0: chid tweets
#       - index 1: place tweets
#       - index 2: health tweets
def joinOldAndNewTweets(origRecords,childRecords,placeRecords,healthRecords):
    joinedTweets = []
    datasets = [childRecords,placeRecords,healthRecords]
    dataCats = [
        ['is_child','age'],
        ['location_cat','location'],
        ['is_health','health_impact','health_type']
    ]

    # for each label category (child,place,health) join tweets labeled before and after label change
    for index in range(len(datasets)):
        nonRedundantNew = removeRedundant(datasets[index],dataCats[index])
        nonRedundantOrig = removeRedundant(origRecords[index],dataCats[index])
        joinedTweets.append(ps.concat([nonRedundantNew,nonRedundantOrig]))
    return(joinedTweets)

# given labels as a string, convert to multiclassification binary array
# INPUTS:
#   childCode (str array) - labels as a string
# OUTPUTS:
#   numpy array of 0s and 1s, one index for each child label (e.g. baby, infant, etc)
def convertChildCodeToLabels(childCode):
    keys = list(childCodingDict.keys())
    codeArr = [0 for x in range(6)]
    for key in keys:
        if key in childCode:
            codeArr[childCodingDict[key]] = 1
    if(sum(codeArr)>=1):
        codeArr[childCodingDict['No specific age (children general)']] = 1
    return(np.asarray(codeArr))

# count number of positive examples for each child labels
# INPUTS:
#    childDataset (pandas dataframe) - contains all tweets with child labels
# OUTPUTS:
#    numpy array with the number of positive examples for each child label
def nChildLabels(childDataset):
    sumArr = [0,0,0,0,0,0]
    for i in range(childDataset.count().iloc[0]):
        curTweet = childDataset.iloc[i]
        codeArr = convertChildCodeToLabels(curTweet['age'])
        sumArr += codeArr
    return(sumArr)

# given labels as a string, convert to multiclassification binary array
# INPUTS:
#    placeCode (str array) - place labels as a string
#    outdoor code (str array) - indoor/outdoor labels as a string
# OUTPUTS:
#    numpy array of 0s and 1s, one index for each place and indoor/outdoor label
def convertPlaceCodeToLabels(placeCode,outdoorCode):
    keys = list(placeCodingDict.keys())
    keys2 = list(inDoorOutdoor.keys())
    codeArr = [0 for x in range(10)]
    for key in keys:
        if key in placeCode:
            codeArr[placeCodingDict[key]] = 1
    for key in keys2:
        if key in outdoorCode:
            codeArr[inDoorOutdoor[key]] = 1
    return(np.asarray(codeArr))

# count number of positive examples for each place and indoor/outdoor label
# INPUTS:
#    placeDataset (pandas dataframe) - contains all tweets with place labels
# OUTPUTS:
#    numpy array with the number of positive examples for each place label
def nPlaceLabels(placeDataset):
    sumArr = [0 for x in range(10)]
    for i in range(placeDataset.count().iloc[0]):
        curTweet = placeDataset.iloc[i]
        codeArr = convertPlaceCodeToLabels(curTweet['location'],curTweet['location_cat'])
        sumArr += codeArr
    return(sumArr)

# given labels as a string, convert to multiclassifiction binary array
# INPUTS:
#    isHealthCode (str array) - whether tweets are labels as a string
#    healthImpactCode (str array) - positive/negative impacts as a code
#    healthTypeCode (str array) - health impact type code
# OUTPUTS:
#    numpy array of 0s and 1s, one index for each healthimpact, type label
def convertHealthCodeToLabels(isHealthCode,healthImpactCode,healthTypeCode):
    keys2 = list(healthImpact.keys())
    keys3 = list(healthType.keys())
    codeArr = [0 for x in range(7)]
    if(isHealthCode==True):
        codeArr[0] = 1
    for key in keys2:
        if key in healthImpactCode:
            codeArr[healthImpact[key]] = 1
    for key in keys3:
        if key in healthTypeCode:
            codeArr[healthType[key]] = 1
    return(np.asarray(codeArr))

# count number of positive examples for each health label
# INPUTS:
#    healthDataset (pandas dataframe) - contains all tweets with health labels
# OUTPUTS:
#    numpy array with the number of positive examples for each health label
def nHealthLabels(healthDataset):
    sumArr = [0 for x in range(7)]
    print(healthDataset.count()[0])
    for i in range(healthDataset.count()[0]):
        curTweet = healthDataset.iloc[i]
        codeArr = convertHealthCodeToLabels(curTweet['is_health'],curTweet['health_impact'],curTweet['health_type'])
        sumArr += codeArr
    return(sumArr)

# main function
def main():

    # create an instance of the custom SQL driver
    SQL_Driver = SQL_DB(dbDict)

    # get twitter records and labels from SQL database
    origRecords,childTweets,placeTweets,healthTweets,labels = getRecordsFromSQL()

    # join twitter records to labels
    origLabeled,childLabeled,placeLabeled,healthLabeled = joinLabelsToRecords(origRecords,childTweets,placeTweets,healthTweets,labels)

    # join tweets recorded before and after the designated label change
    childJoined,placeJoined,healthJoined = joinOldAndNewTweets(origLabeled,childLabeled,placeLabeled,healthLabeled)
    
    # calculate n positive labels for each category
    nChildLabels(childJoined) 
    # [ baby: 6589,  toddler: 4340,  elem: 1887,  middle: 1903,  high: 3488, unknown: 23307]
    childJoined.to_csv('insert output filepath',index=False)

    # calculate n positive labels for each category
    nPlaceLabels(placeJoined)
    # [ childcare: 2278,  park: 3074, home: 12298, school: 8760, neighborhood: 1805,
    #   other: 19697, unsure: 38639, indoor: 28345, outdoor: 13608, unsure: 43147]
    placeJoined.to_csv('insert output filepath',index=False)

    # calculate health training data summary statistics and save
    nHealthLabels(healthJoined)
    # [ negative: 18611, positive: 12624, no impact: 1130,
    #   cognitive: 3470, emotional: 17079, physical: 13820]
    healthJoined.to_csv('insert output filepath',index=False)

# end of filterTrainingRecords.py