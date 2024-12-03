#createTFRecordsFromLabels.py
#Author: Andrew Larkin

# Summary:  given a csv file of deep learning train and test records,
#           convert records to TFRecords format and save to disk

# import libraries
import os
import numpy as np
import tensorflow as tf
from transformers import *
import pandas as ps

# import secrets and custom classes
from mySecrets import secrets
from ChildData_Class import ChildData
from PlaceData_Class import PlaceData
from HealthData_Class import HealthData
from mySecrets import secrets

# create tf records from text files
# INPUTS:
#    dataTuple (tuple) - contains data needed to create an instance of the data prepper class
#        index 0 - pandas dataframe containing inputs and labels for the deep learning model
#        index 1 - folderpath where model weights should be loaded and stored
#        index 2 - absolute filepath where the custom BERT tokenizer is stored
#        index 3 - summary document vectors for the text author
#    labelType (str) - indicates which type of deep learning model to create tf records for (age,place, or health)
def convertTextFileToTFRecord(dataTuple,labelType):

    # create an object for writing TFRecords for the child model
    if(labelType=='age'):
        dataPrepper = ChildData(dataTuple[1],dataTuple[2])

    # create an object for writing TFRecords for the place model
    elif(labelType=='place'):
            dataPrepper = PlaceData(dataTuple[1],dataTuple[2])

    # create an object for writing TFRecords for the health model
    else:
            dataPrepper = HealthData(dataTuple[1],dataTuple[2])
    dataPrepper.convertTextFileToTFRecord(dataTuple[0],dataTuple[3])

# load social media text and metadata from csv file. Partition records into train and test datasets
# INPUTS:
#    textFilepath (str) absolute filepath for csv file
#    vectorFilepath (str) - absolute filepath where author summary vectors are stored as .npy
#    outFilepath (str) - absolute filepath where input csv appended with 'train' and 'test' indicator 
#                        will be stored. For debugging/evaluation purposes only
# OUTPUTS:
#    tweetData (pandas dataframe) - social media text and metadata 
#    selectedVectors (np float 32 matrix) - author summary vectors, with the same indexes as the tweetData output
def loadTextData(textFilepath,vectorFilepath,outFilepath):

    # read social media posts from csv
    tweetData = ps.read_csv(textFilepath,encoding='utf-8',lineterminator='\n')
    tweetData['vectorIndex'] = tweetData.index
    tweetData = tweetData.drop_duplicates()

    # randomly shuffle the social media posts and partition the first 5000 for model evaluation
    tweetData = tweetData.sample(frac=1)
    tweetDataTest = tweetData.iloc[0:5000]
    tweetDataTest['test'] = 1
    tweetDataTrain = tweetData.iloc[5000:]
    tweetDataTrain['test'] = 0
    tweetData = ps.concat([tweetDataTest,tweetDataTrain])
    tweetData.to_csv(outFilepath,index=False,encoding='utf-8')

    # load author vectors and sort so the order matches the social media posts
    authorVectors = np.load(vectorFilepath)
    selectedVectors = []
    tweetData.columns = tweetData.columns.str.replace('\r', '')
    for index in list(tweetData['vectorIndex']):
         selectedVectors.append(authorVectors[:,index])

    return([tweetData,selectedVectors])

# main function
if __name__ == '__main__':

    # create tf records for the child deep learning model
    childDataset,authorVectors = loadTextData(
        secrets['CHILD_LABELS_FILEPATH'],
        secrets['CHILD_AUTHOR_VECTORS_FILEPATH'],
        secrets['CHILD_TF_RECORDS_FILEPATH']
    )
    print("%i records in child training dataset" %(childDataset.count().iloc[0]))
    convertTextFileToTFRecord((childDataset, secrets['CHILD_FOLDER'],secrets['TOKEN_PATH'],authorVectors),'age')

    # create tf records for the place deep learning model
    placeDataset,authorVectors = loadTextData(
        secrets['PLACE_LABELS_FILEPATH'],
        secrets['PLACE_AUTHOR_VECTORS_FILEPATH'],
        secrets['PLACE_TF_RECORDS_FILEPATH']
    )
    print("%i records in place training dataset" %(placeDataset.count().iloc[0]))
    convertTextFileToTFRecord((placeDataset, secrets['PLACE_FOLDER'],secrets['TOKEN_PATH'],authorVectors),'place')

    # create tf records for the health deep learning model
    healthDataset,authorVectors = loadTextData(
        secrets['HEALTH_LABELS_FILEPATH'],
        secrets['HEALTH_AUTHOR_VECTORS_FILEPATH'],
        secrets['HEALTH_TF_RECORDS_FILEPATH']
    )
    print("%i records in health dataset" %(placeDataset.count().iloc[0]))
    convertTextFileToTFRecord((healthDataset, secrets['HEALTH_FOLDER'],secrets['TOKEN_PATH'],authorVectors),'health')

# end of createTFRecordsFromLabels.py