import os
import numpy as np
import tensorflow as tf
from transformers import *
import pandas as ps
from secrets import secrets

PARENT_FOLDER = "/mnt/h/Aspire/BERT/"
TEXT_FOLDER = PARENT_FOLDER + "BERT_text/"
TF_RECORDS_FOLDER = PARENT_FOLDER + "tfRecords/"
TOKEN = BertTokenizer.from_pretrained('expandedTokenBase')


class TFRecordMaker():
    def __init__(self):
        self.nTest = 5000
    
    def bytes_feature(self,value):
        """Returns a bytes_list from a string / byte."""
        if isinstance(value, type(tf.constant(0))): # if value is tensor
            value = value.numpy() # get value of tensor
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))
    
    def float_feature(self,value):
        """Returns a floast_list from a float / double."""
        return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))

    def int64_feature(self,value):
        """Returns an int64_list from a bool / enum / int / uint."""
        return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

    def serialize_array(self,array):
        array = tf.io.serialize_tensor(array)
        return array


    def parse_single_tweet(self,input_ids,masks,labels):
        #define the dictionary -- the structure -- of our single example
        data = {
            'input_ids':self.bytes_feature(self.serialize_array(input_ids)),
            'masks':self.bytes_feature(self.serialize_array(masks)),
            'labels':self.bytes_feature(self.serialize_array(labels))
        }
        out = tf.train.Example(features=tf.train.Features(feature=data))
        return(out)
    
    def write_tweet_text_to_tfr_short(self,tweetDict,origfilename):
        inputIds = tweetDict['input_ids']
        attentionMask = tweetDict['attention_mask']
        labels = tweetDict['labels']
        count = 1
        
        trainFilename= origfilename+  "childDataTrain.tfrecords"
        testFilename = origfilename + "childDataTest.tfrecords"
        testWriter = tf.io.TFRecordWriter(testFilename) #create a writer that'll store our data to disk
        trainWriter = tf.io.TFRecordWriter(trainFilename)
        print("%i records to process " %(len(inputIds)))
        print("writing to file %s" %(testFilename))
        for index in range(len(inputIds)):
            if(index<self.nTest):
                curWriter = testWriter
            else:
                curWriter = trainWriter
            try:
                out = self.parse_single_tweet(inputIds[index],attentionMask[index],labels[index])
                curWriter.write(out.SerializeToString())
                count += 1
                index+=1
            except Exception as e:
                print(str(e))
                        
        testWriter.close()
        trainWriter.close()
        print(f"Wrote {count} elements to TFRecords")
        return count
    

    
class dlDataPrepper():
    def __init__(self,inFilepath,outFilepath):  
        self.TFRecordWriter = TFRecordMaker()
        self.tweetFilepath = inFilepath
        self.outFilepath = outFilepath
        self.tokenizer = TOKEN
        self.childCodingDict = {
            #'No children':0,
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

    
def convertTextFileToTFRecord2(dataTuple):
    dataPrepper = dlDataPrepper('a',dataTuple[1])
    dataPrepper.convertTextFileToTFRecord2(dataTuple[0])


def loadTweetText2(tweetFilepath):
    tweetData = ps.read_csv(tweetFilepath,encoding='utf-8',lineterminator='\n')
    tweetData = tweetData.drop_duplicates()
    tweetDataTest = tweetData[tweetData['test']== 1]
    tweetDataTrain = tweetData[tweetData['test']==0]
    #isElem = tweetDataTrain[tweetDataTrain['isElem'] == 1]
    #tweetDataTrain = ps.concat([tweetDataTrain,isElem])
    tweetDataTrain = tweetDataTrain.sample(frac=1)
    tweetData = ps.concat([tweetDataTest,tweetDataTrain])
    tweetData.to_csv("/mnt/h/Aspire/BERT/child/childTweetsStrict.csv",index=False,encoding='utf-8')

    print(tweetData.head())
    print(tweetData.count()[0])
    #tweetData = tweetData.sample(frac=1)
    return(tweetData)

if __name__ == '__main__':
    childDataset = loadTweetText2('/mnt/h/Aspire/BERT/child/childTweetsAug.csv')
    print("%i tweets in dataset" %(childDataset.count()[0]))
    convertTextFileToTFRecord2((childDataset, '/mnt/h/Aspire/BERT/child/'))

