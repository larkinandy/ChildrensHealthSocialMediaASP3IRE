import numpy as np
import tensorflow as tf
from transformers import *
from TFRecordWrite_Class import TFRecordWrite

class PlaceData():
    def __init__(self,outFilepath,TokenPath):  
        self.TFRecordWriter = TFRecordWrite(5000)
        self.outFilepath = outFilepath
        self.tokenizer = BertTokenizer.from_pretrained(TokenPath)
        self.placeCodingDict = {
            'Childcare/daycare':0,
            'Park/playground/child sports center':1,
            'A home':2,
            'School':3,
            'Neigborhood (but not on home property, etc)':4,
            'Other':5,
            'Unsure':5,
            'No location':6
        }
        self.inDoorOutdoor = {
            'Indoor location':7,
            'Outdoor location':8,
            'No location/unsure':9
        }

    def convertPlaceCodeToLabels(self,record):
        keys = list(self.placeCodingDict.keys())
        keys2 = list(self.inDoorOutdoor.keys())
        codeArr = [0 for x in range(10)]
        for key in keys:
            if key in record['location']:
                codeArr[self.placeCodingDict[key]] = 1
        for key in keys2:
            if key in record['location_cat']:
                codeArr[self.inDoorOutdoor[key]] = 1
        return(np.asarray(codeArr))
 
    def mapNewLineReplace(self,origLine):
        newLine = origLine.replace("\n",". ")
        return(newLine)
    
    def convertUserStatsToArray(self,userDF):
        codeArr = [0.0 for x in range(7)]
        nPosted = userDF['nPosted']
        codeArr[0] = userDF['nDaycare']/nPosted
        codeArr[1] = userDF['nPark']/nPosted
        codeArr[2] = userDF['nHome']/nPosted
        codeArr[3] = userDF['nSchool']/nPosted
        codeArr[4] = userDF['nNeighborhood']/nPosted
        codeArr[5] = userDF['nIndoor']/nPosted
        codeArr[6] = userDF['nOutdoor']/nPosted
        return(np.asarray(codeArr))
        
    def setupModelInputs(self,tweetData,vectorData):
        tweetText = list(tweetData['text'])
        tweetText = list(map(self.mapNewLineReplace,tweetText))
        tweetLabels = tweetData.apply(self.convertPlaceCodeToLabels,axis=1)
        userStats = tweetData.apply(self.convertUserStatsToArray,axis=1)
        inputs = self.tokenizer(tweetText,max_length=100,truncation=True,padding='max_length',return_tensors="tf")
        inp_ids = tf.convert_to_tensor(inputs['input_ids'])
        inputs['input_ids'] = inp_ids
        inputs['labels'] = tweetLabels
        inputs['vectors'] = vectorData
        inputs['user_stats'] = userStats
        return(inputs)

    def convertTextFileToTFRecord(self,tweetArray,vectorArray):
        modelInputs = self.setupModelInputs(tweetArray,vectorArray)
        print("completed creating model inputs")
        self.TFRecordWriter.write_tweet_text_to_tfr_short(modelInputs,self.outFilepath,'place')