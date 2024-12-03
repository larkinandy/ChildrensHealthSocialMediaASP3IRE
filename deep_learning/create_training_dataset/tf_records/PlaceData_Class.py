# PlacedData_Class.py
# Author: Andrew Larkin

# Summary: Custom Data class for cleaning and processing 'Place'-labeled social media posts into tf records 
#          for training deep learning models

# import libraries
import numpy as np
import tensorflow as tf
from transformers import *

# import custom classes
from TFRecordWrite_Class import TFRecordWrite

class PlaceData():
    # create instance of place data class
    # INPUTS:
    #    outputFilepath (str) - absolute filepath where tf records will be stored
    #    TokenPath (str) - absolute fiepath where custom BERT tokenizer is stored
    def __init__(self,outFilepath,TokenPath):  

        # create TFRecordWrite object for writing tf records. #5000 is the test dataset size
        self.TFRecordWriter = TFRecordWrite(5000)
        self.outFilepath = outFilepath

        # load BERT tokenizer from disk
        self.tokenizer = BertTokenizer.from_pretrained(TokenPath)

        # kv pairs for converting place type labels into binary array
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

        # kv pairs for converting indoor/outdoor labels into binary array
        self.inDoorOutdoor = {
            'Indoor location':7,
            'Outdoor location':8,
            'No location/unsure':9
        }

    # given place labels in text format, convert to binary array labels 
    # INPUTS:
    #    placeCode (str) - place labels in text format
    # OUTPUTS:
    #    place labels in binary np array format
    def convertPlaceCodeToLabels(self,placeCode):

        # get possible text labels
        keys = list(self.placeCodingDict.keys())
        keys2 = list(self.inDoorOutdoor.keys())

        # initialize output array with 0s. 
        codeArr = [0 for x in range(10)]

        # for each possible text label, test is label is in label assigned by worker 
        # and if so change the corresponding digit in the output array to 1
        for key in keys:
            if key in placeCode['location']:
                codeArr[self.placeCodingDict[key]] = 1
        for key in keys2:
            if key in placeCode['location_cat']:
                codeArr[self.inDoorOutdoor[key]] = 1
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
    #    np array, with percentages of each health label in a seperate inde
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
        
    # given social media post labels and metadata, transform the data into arrays for tf records
    # INPUTS:
    #    tweetData (pandas dataframe) - contains social media post text, labels, and metadata
    #    vectorData (np array) - document vectors for social media authors
    # OUTPUTS:
    #    pandas array, with input records preprocessed for saving as tf records
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

    # create tf records from social media and author data
    # INPUTS:
    #    tweetArray (pandas dataframe) - social media labels and metadata
    #    vectorArray (np array) - document vectors for social media authors
    def convertTextFileToTFRecord(self,tweetArray,vectorArray):
        modelInputs = self.setupModelInputs(tweetArray,vectorArray)
        print("completed creating model inputs")
        self.TFRecordWriter.write_tweet_text_to_tfr_short(modelInputs,self.outFilepath,'place')

# end of PlaceData_Class.py