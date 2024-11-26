

import tensorflow as tf
#from transformers import *

class TFRecordWrite():
    def __init__(self,nTest):
        self.nTest = nTest

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


    def parse_single_tweet(self,input_ids,masks,labels,vectors,user_stats):
        #define the dictionary -- the structure -- of our single example
        data = {
            'input_ids':self.bytes_feature(self.serialize_array(input_ids)),
            'masks':self.bytes_feature(self.serialize_array(masks)),
            'labels':self.bytes_feature(self.serialize_array(labels)),
            'vectors':self.bytes_feature(self.serialize_array(vectors)),
            'user_stats':self.bytes_feature(self.serialize_array(user_stats)),
        }
        out = tf.train.Example(features=tf.train.Features(feature=data))
        return(out)
    
    def write_tweet_text_to_tfr_short(self,tweetDict,origfilename,cat):
        inputIds = tweetDict['input_ids']
        attentionMask = tweetDict['attention_mask']
        labels = tweetDict['labels']
        vectors = tweetDict['vectors']
        userStats = tweetDict['user_stats']
        trainCount,testCount = 0,0
        
        trainFilename= origfilename + cat + "DataTrain.tfrecords"
        testFilename = origfilename + cat + "DataTest.tfrecords"
        testWriter = tf.io.TFRecordWriter(testFilename) #writer to store test records to disk
        trainWriter = tf.io.TFRecordWriter(trainFilename) # writer to store train records to disk
        print("%i records to process " %(len(inputIds)))
        print("writing to file %s" %(testFilename))
        for index in range(len(inputIds)):
            if(index<self.nTest):
                curWriter = testWriter
                testCount +=1
            else:
                curWriter = trainWriter
                trainCount +=1
            try:
                out = self.parse_single_tweet(inputIds[index],attentionMask[index],labels[index],vectors[index],userStats[index])
                curWriter.write(out.SerializeToString())
                index+=1
            except Exception as e:
                print(str(e))
                        
        testWriter.close()
        trainWriter.close()
        print("wrote %i train and %i test TFRecords" %(trainCount,testCount))
        return (trainCount + testCount)
    
# end of TFRecordMaker.py