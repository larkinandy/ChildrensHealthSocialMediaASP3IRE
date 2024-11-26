import os
import numpy as np
import tensorflow as tf
from transformers import *
import pandas as ps
from mySecrets import secrets
from TFRecordWrite_Class import TFRecordWrite
from ChildData_Class import ChildData
from PlaceData_Class import PlaceData
from HealthData_Class import HealthData
from mySecrets import secrets


def convertTextFileToTFRecord(dataTuple):
    dataPrepper = ChildData(dataTuple[1],dataTuple[2])
    dataPrepper.convertTextFileToTFRecord(dataTuple[0],dataTuple[3])


def loadTextData(textFilepath,vectorFilepath,outFilepath):
    tweetData = ps.read_csv(textFilepath,encoding='utf-8',lineterminator='\n')
    tweetData['vectorIndex'] = tweetData.index
    tweetData = tweetData.drop_duplicates()
    tweetData = tweetData.sample(frac=1)
    tweetDataTest = tweetData.iloc[0:5000]
    tweetDataTest['test'] = 1
    tweetDataTrain = tweetData.iloc[5000:]
    tweetDataTrain['test'] = 0
    tweetData = ps.concat([tweetDataTest,tweetDataTrain])
    tweetData.to_csv(outFilepath,index=False,encoding='utf-8')
    authorVectors = np.load(vectorFilepath)
    selectedVectors = []
    tweetData.columns = tweetData.columns.str.replace('\r', '')
    for index in list(tweetData['vectorIndex']):
         selectedVectors.append(authorVectors[:,index])

    return([tweetData,selectedVectors])

if __name__ == '__main__':
    childDataset,authorVectors = loadTextData(
        secrets['CHILD_LABELS_FILEPATH'],
        secrets['CHILD_AUTHOR_VECTORS_FILEPATH'],
        secrets['CHILD_TF_RECORDS_FILEPATH']
    )
    print("%i records in child training dataset" %(childDataset.count().iloc[0]))
    convertTextFileToTFRecord((childDataset, secrets['CHILD_FOLDER'],secrets['TOKEN_PATH'],authorVectors))

    placeDataset,authorVectors = loadTextData(
        secrets['PLACE_LABELS_FILEPATH'],
        secrets['PLACE_AUTHOR_VECTORS_FILEPATH'],
        secrets['PLACE_TF_RECORDS_FILEPATH']
    )
    print("%i records in place training dataset" %(placeDataset.count().iloc[0]))
    convertTextFileToTFRecord((placeDataset, secrets['PLACE_FOLDER'],secrets['TOKEN_PATH'],authorVectors))

    healthDataset,authorVectors = loadTextData(
        secrets['HEALTH_LABELS_FILEPATH'],
        secrets['HEALTH_AUTHOR_VECTORS_FILEPATH'],
        secrets['HEALTH_TF_RECORDS_FILEPATH']
    )
    print("%i records in health dataset" %(placeDataset.count().iloc[0]))
    convertTextFileToTFRecord((healthDataset, secrets['HEALTH_FOLDER'],secrets['TOKEN_PATH'],authorVectors))

