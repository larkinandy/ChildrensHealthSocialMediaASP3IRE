import numpy as np
import tensorflow as tf
from transformers import *
from TFRecord_Class import TFRecord

class ChildData():
    def __init__(self,inFilepath,outFilepath,TokenPath):  
        self.TFRecordWriter = TFRecord()
        self.tweetFilepath = inFilepath
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

    def multiplyGroupByVal(data,multipleNums = [2,2,4,2,2]):
        dataSubsets = []
        blankArr = [0,0,0,0,0,0]
        noCode = data[data['ageCode']==blankArr]
        dataSubsets.append(noCode)
        
    def setupModelInputs(self,tweetData,debug=False):
        tweetText = list(tweetData['text'])
        tweetText = list(map(self.mapNewLineReplace,tweetText))
        tweetLabels = list(map(self.convertChildCodeToLabels,list(tweetData['age'])))
        inputs = self.tokenizer(tweetText,max_length=100,truncation=True,padding='max_length',return_tensors="tf")
        inp_ids = tf.convert_to_tensor(inputs['input_ids'])
        inputs['input_ids'] = inp_ids
        inputs['labels'] = tweetLabels
        return(inputs)

    def convertTextFileToTFRecord2(self,tweetArray):
        modelInputs = self.setupModelInputs(tweetArray)
        print("completed creating model inputs")
        self.TFRecordWriter.write_tweet_text_to_tfr_short(modelInputs,self.outFilepath)