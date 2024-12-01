import numpy as np
import tensorflow as tf
from transformers import *
from TFRecordWrite_Class import TFRecordWrite

class ChildData():
    def __init__(self,outFilepath,TokenPath):  
        self.TFRecordWriter = TFRecordWrite(5000)
        self.outFilepath = outFilepath
        self.tokenizer = BertTokenizer.from_pretrained(TokenPath)
        self.childCodingDict = {
            '0 to less than 1 year (baby/infant)':0,
            '1 to 4 years (toddler/pre-school)':1,
            '5 to 10 years (elementary school)':2,
            '11 to 13 years (middle school)':3,
            '14 to 17 years (high school)':4,
            'School age (no specific school type)':5,
            'No specific age (children general)':5,
            'Unsure':5
        }

    def convertChildCodeToLabels(self,childCode):
        keys = list(self.childCodingDict.keys())
        codeArr = [0 for x in range(6)]
        for key in keys:
            if key in childCode:
                codeArr[self.childCodingDict[key]] = 1
        if(sum(codeArr)>=1):
            codeArr[self.childCodingDict['No specific age (children general)']] = 1
        return(np.asarray(codeArr))
 
    def mapNewLineReplace(self,origLine):
        newLine = origLine.replace("\n",". ")
        return(newLine)
    
    def convertUserStatsToArray(self,userDF):
        codeArr = [0.0 for x in range(6)]
        nPosted = userDF['nPosted']
        codeArr[0] = userDF['nChild']/nPosted
        codeArr[1] = userDF['nBaby']/nPosted
        codeArr[2] = userDF['nToddler']/nPosted
        codeArr[3] = userDF['nElem']/nPosted
        codeArr[4] = userDF['nMiddle']/nPosted
        codeArr[5] = userDF['nHigh']/nPosted
        return(np.asarray(codeArr))
        
    def setupModelInputs(self,tweetData,vectorData):
        tweetText = list(tweetData['text'])
        tweetText = list(map(self.mapNewLineReplace,tweetText))
        tweetLabels = list(map(self.convertChildCodeToLabels,list(tweetData['age'])))
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
        self.TFRecordWriter.write_tweet_text_to_tfr_short(modelInputs,self.outFilepath,'child')