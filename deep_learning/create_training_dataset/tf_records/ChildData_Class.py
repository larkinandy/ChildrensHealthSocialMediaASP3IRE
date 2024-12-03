# ChildData_Class.py
# Author: Andrew Larkin

# Summary: Custom Data class for cleaning and processing 'Child'-labeled social media posts into tf records 
#          for training deep learning models

# import libraries
import numpy as np
import tensorflow as tf
from transformers import *

# import custom classes
from TFRecordWrite_Class import TFRecordWrite


class ChildData():
    # create instance of child data class
    # INPUTS:
    #    outputFilepath (str) - absolute filepath where tf records will be stored
    #    TokenPath (str) - absolute fiepath where custom BERT tokenizer is stored
    def __init__(self,outFilepath,TokenPath):  

        # create TFRecordWrite object for writing tf records. #5000 is the test dataset size
        self.TFRecordWriter = TFRecordWrite(5000)
        self.outFilepath = outFilepath

        # load BERT tokenizer from disk
        self.tokenizer = BertTokenizer.from_pretrained(TokenPath)

        # kv pairs for converting labels into binary array
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

    # given child labels in text format, convert to binary array labels 
    # INPUTS:
    #    childCode (str) - child labels in text format
    # OUTPUTS:
    #    child labels in binary np array format
    def convertChildCodeToLabels(self,childCode):

        # get possible text labels
        keys = list(self.childCodingDict.keys())

        # initialize output array with 0s
        codeArr = [0 for x in range(6)]

        # for each possible text label, test is label is in label assigned by worker 
        # and if so change the corresponding digit in the output array to 1
        for key in keys:
            if key in childCode:
                codeArr[self.childCodingDict[key]] = 1

        # if any of the text labels are present, change the digit corresponding to
        # 'any chid' to 1
        if(sum(codeArr)>=1):
            codeArr[self.childCodingDict['No specific age (children general)']] = 1
        return(np.asarray(codeArr))
 
    # replace new line characters with a period and space
    # INPUTS:
    #    origLine (str) - line to replace characters in
    # OUTPUTS:
    #    newLine (str) - line with new line characters replaced with ". "
    def mapNewLineReplace(self,origLine):
        newLine = origLine.replace("\n",". ")
        return(newLine)
    
    # convert statistics of the author who posted the social media post to 
    # a np array
    # INPUTS:
    #    userDF (pandas dataframe) - statistics of the social media post author
    # OUTPUTS:
    #    np array, with percentages of each child label in a seperate index
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
    
    # given social media post labels and metadata, transform the data into arrays for tf records
    # INPUTS:
    #    tweetData (pandas dataframe) - contains social media post text, labels, and metadata
    #    vectorData (np array) - document vectors for social media authors
    # OUTPUTS:
    #    pandas array, with input records preprocessed for saving as tf records
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

    # create tf records from social media and author data
    # INPUTS:
    #    tweetArray (pandas dataframe) - social media labels and metadata
    #    vectorArray (np array) - document vectors for social media authors
    def convertTextFileToTFRecord(self,tweetArray,vectorArray):
        modelInputs = self.setupModelInputs(tweetArray,vectorArray)
        print("completed creating model inputs")
        self.TFRecordWriter.write_tweet_text_to_tfr_short(modelInputs,self.outFilepath,'child')

# end of ChildData_Class.py