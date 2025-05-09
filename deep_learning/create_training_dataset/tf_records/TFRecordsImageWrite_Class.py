# TFRecordsImageWrite_Class.py
# Author: Andrew Larkin

# Summary: create tf records for models that use imagary as inputs

# import libraries
import os
import numpy as np
import tensorflow as tf
from transformers import *
import pandas as ps


class TFRecordsImage():

    # initialize class. Folder locations were initially passed during initialization,
    # but later passed as input argument during function calls
    def __init__(self):
        print("created TFRecordsImage object")
    
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

    # create tf record from an image and label
    # INPUTS:
    #    imageMatrix (np array) - rgb image stored in numpy 3d matrix
    #    label (int list) - one-hot encoding for image label
    # OUTPUTS:
    #    tf record for image
    def parse_single_image(self,imageMatrix,label):
        #define the dictionary -- the structure -- of our single example
        data = {
            'image':self.bytes_feature(self.serialize_array(imageMatrix)),
            'label' : self.int64_feature(label),
            'height' : self.int64_feature(imageMatrix.shape[0]),
            'width' : self.int64_feature(imageMatrix.shape[1]),
            'depth' : self.int64_feature(imageMatrix.shape[2]),
        }
        out = tf.train.Example(features=tf.train.Features(feature=data))
        return(out)
    
    # create one hot encoding for age of child in image
    # INPUTS:
    #    age (int) - child age in years (0=newborn)
    # OUTPUTS:
    #    one-hot encoding of child age group
    def convertAgeToClassArray(self,age):
        # indexes [baby,toddler,elementary,middle,high,adult,none]
        if(age<0):
            return [0,0,0,0,0,0,1]
        elif(age<2):
            return [1,0,0,0,0,0,0]
        elif(age<5):
            return [0,1,0,0,0,0,0]
        elif(age<11):
            return [0,0,1,0,0,0,0]
        elif(age<14):
            return [0,0,0,1,0,0,0]
        elif(age<18):
            return [0,0,0,0,1,0,0]
        else:
            return [0,0,0,0,0,1,0]
        
    # write tf records for images in a dictionary. Create a single tf record for each original
    # social media post. Note that a single social media post may contain multiple face clips (images)
    # INPUTS:
    #    faceDict (pandas dataframe) - contains image values (as np matrix) and metadata)
    #    origFilename (str) - filename of the image that corresponds to the metadata
    # OUTPUTS:
    #    count (int) - number of images written to the tf record
    def write_tweet_image_to_tfr_short(self,faceDict,origfilename):
        images = faceDict['image']
        labels = faceDict['label']
        count = 1
        
        filename= origfilename+  "faceAge.tfrecords"
        writer = tf.io.TFRecordWriter(filename) #create a writer that'll store our data to disk
        print("%i records to process " %(len(images)))
        print("writing to file %s" %(filename))
        for index in range(len(images)):
            try:
                out = self.parse_single_image(images[index],labels[index])
                writer.write(out.SerializeToString())
                count += 1
                index+=1
            except Exception as e:
                print(str(e))
                        
        writer.close()
        print(f"Wrote {count} elements to TFRecords")
        return count
    
# end of TFRecordsImageWrite_Class.py