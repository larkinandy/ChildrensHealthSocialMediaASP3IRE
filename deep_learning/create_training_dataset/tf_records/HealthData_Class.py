import numpy as np
import tensorflow as tf
from transformers import *
from TFRecordWrite_Class import TFRecordWrite

class HealthData():
    def __init__(self,outFilepath,TokenPath):  
        self.TFRecordWriter = TFRecordWrite(5000)
        self.outFilepath = outFilepath
        self.tokenizer = BertTokenizer.from_pretrained(TokenPath)
        self.healthImpact = {
            'Negative impact':1,
            'Positive impact':2,
            'No impact':3
        }
        self.healthType = {
            'Cognitive health':4,
            'Emotional/social health':5,
            'Physical health':6
        }

    def convertHealthCodeToLabels(self,record):
        keys2 = list(self.healthImpact.keys())
        keys3 = list(self.healthType.keys())
        codeArr = [0 for x in range(7)]
        if(record['is_health']==True):
            codeArr[0] = 1
        for key in keys2:
            if key in record['health_impact']:
                codeArr[self.healthImpact[key]] = 1
        for key in keys3:
            if key in record['health_type']:
                codeArr[self.healthType[key]] = 1
        return(np.asarray(codeArr))
 
    def mapNewLineReplace(self,origLine):
        newLine = origLine.replace("\n",". ")
        return(newLine)
    
    def convertUserStatsToArray(self,userDF):
        codeArr = [0.0 for x in range(5)]
        nPosted = userDF['nPosted']
        codeArr[0] = userDF['nNegative']/nPosted
        codeArr[1] = userDF['nPositive']/nPosted
        codeArr[2] = userDF['nCognitive']/nPosted
        codeArr[3] = userDF['nPhysical']/nPosted
        codeArr[4] = userDF['nEmotional']/nPosted
        return(np.asarray(codeArr))
        
    def setupModelInputs(self,tweetData,vectorData):
        tweetText = list(tweetData['text'])
        tweetText = list(map(self.mapNewLineReplace,tweetText))
        tweetLabels = tweetData.apply(self.convertHealthCodeToLabels,axis=1)
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
        self.TFRecordWriter.write_tweet_text_to_tfr_short(modelInputs,self.outFilepath,'health')