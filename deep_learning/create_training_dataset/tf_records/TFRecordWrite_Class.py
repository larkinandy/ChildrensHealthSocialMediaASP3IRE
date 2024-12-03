# TFRecordWrite_Class.py
# Author: Andrew Larkin

# Summary: write text-based tf records to train deep learning models

# import libraries
import tensorflow as tf


class TFRecordWrite():

    # create instance of TFRecordWRite class
    # INPUTS:
    #    nTest (int) - number of test records 
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

    # create tf record from single social media text and metadata
    # INPUTS:
    #    input_ids (np float array) - BERT tokens for words in social media text
    #    masks (np int array) - flags tokens that are after the end of text
    #    labels (np int array) - one hot encoding for record label
    #    vectors (np float array) - author summary vector
    #    user_stats (np float array) - author summary stats (e.g. n posts about children)
    # OUTPUTS:
    #    input data transformed into tf record
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
    
    # create tf records from a set of text-based social media posts
    # INPUTS:
    #    tweetDict (pandas dataframe) - contains text and metadata
    #    origFilename (str) - filename of csv file that contained post info
    #    cat (str) - type of deep learning models records are being created for (age,place,or health)
    # OUTPUTS:
    #    number of tf records that were written (int)
    def write_tweet_text_to_tfr_short(self,tweetDict,origfilename,cat):

        # get tokenized social media text
        inputIds = tweetDict['input_ids']

        # get mask for end of post
        attentionMask = tweetDict['attention_mask']

        # get labels
        labels = tweetDict['labels']

        # get author summary vectors and user stats
        vectors = tweetDict['vectors']
        userStats = tweetDict['user_stats']

        # script creates seperate tf record files for train and test datasets
        trainCount,testCount = 0,0
        trainFilename= origfilename + cat + "DataTrain.tfrecords"
        testFilename = origfilename + cat + "DataTest.tfrecords"
        testWriter = tf.io.TFRecordWriter(testFilename) #writer to store test records to disk
        trainWriter = tf.io.TFRecordWriter(trainFilename) # writer to store train records to disk
        print("%i records to process " %(len(inputIds)))
        print("writing to file %s" %(testFilename))

        # write social media posts to disk. Use the testWriter for test records and trainWriter
        # for training records
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