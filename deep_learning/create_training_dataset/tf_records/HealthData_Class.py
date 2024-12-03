# HealthdData_Class.py
# Author: Andrew Larkin

# Summary: Custom Data class for cleaning and processing 'Health'-labeled social media posts into tf records 
#          for training deep learning models

# import libraries
import numpy as np
import tensorflow as tf
from transformers import *

# import custom classes
from TFRecordWrite_Class import TFRecordWrite


class HealthData():
    # create instance of health data class
    # INPUTS:
    #    outputFilepath (str) - absolute filepath where tf records will be stored
    #    TokenPath (str) - absolute fiepath where custom BERT tokenizer is stored
    def __init__(self,outFilepath,TokenPath):  

        # create TFRecordWrite object for writing tf records. #5000 is the test dataset size
        self.TFRecordWriter = TFRecordWrite(5000)
        self.outFilepath = outFilepath

        # load BERT tokenizer from disk
        self.tokenizer = BertTokenizer.from_pretrained(TokenPath)

        # kv pairs for converting health impact labels into binary array
        self.healthImpact = {
            'Negative impact':1,
            'Positive impact':2,
            'No impact':3
        }

        # kv pairs for converting health type labels into binary array
        self.healthType = {
            'Cognitive health':4,
            'Emotional/social health':5,
            'Physical health':6
        }

    # given health labels in text format, convert to binary array labels 
    # INPUTS:
    #    rehealthCode (str) - health labels in text format
    # OUTPUTS:
    #    health labels in binary np array format
    def convertHealthCodeToLabels(self,healthCode):

        # get possible text labels
        keys2 = list(self.healthImpact.keys())
        keys3 = list(self.healthType.keys())

        # initialize output array with 0s. Set index 0 to 1 if the post is about health
        codeArr = [0 for x in range(7)]
        if(healthCode['is_health']==True):
            codeArr[0] = 1

        # for each possible text label, test is label is in label assigned by worker 
        # and if so change the corresponding digit in the output array to 1
        for key in keys2:
            if key in healthCode['health_impact']:
                codeArr[self.healthImpact[key]] = 1
        for key in keys3:
            if key in healthCode['health_type']:
                codeArr[self.healthType[key]] = 1
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
    #    np array, with percentages of each health label in a seperate index
    def convertUserStatsToArray(self,userDF):
        codeArr = [0.0 for x in range(5)]
        nPosted = userDF['nPosted']
        codeArr[0] = userDF['nNegative']/nPosted
        codeArr[1] = userDF['nPositive']/nPosted
        codeArr[2] = userDF['nCognitive']/nPosted
        codeArr[3] = userDF['nPhysical']/nPosted
        codeArr[4] = userDF['nEmotional']/nPosted
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
        tweetLabels = tweetData.apply(self.convertHealthCodeToLabels,axis=1)
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
        self.TFRecordWriter.write_tweet_text_to_tfr_short(modelInputs,self.outFilepath,'health')

# end of HealthData_Class.py