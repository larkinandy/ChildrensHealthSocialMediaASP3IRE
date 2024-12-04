# TFRecordRead_Class.py
# Author: Andrew Larkin

# Summary: Read TFRecords for training deep learning models

# import libraries
import tensorflow as tf
import os
import glob
import numpy as np


class TFRecordRead():
    
    # create instance of TFRecordRead
    # INPUTS:
    #    dataFolder (str) - folderpath where tf records are stored
    #    batchSize (int) - deep learning model batch size
    #    vector (Boolean) - whether to include author summary vectors in loaded tf records
    #    userStats (Boolean) - whether to include author statistics in loaded tf records
    def __init__(self,dataFolder,batchSize,vector=False,userStats=False):
        self.dataFolder = dataFolder
        self.vector = vector
        self.userStats = userStats
        self.tfRecords = self.load_dataset(dataFolder,'*.tfrecords')
        self.batch_size = batchSize
        self.AUTOTUNE = tf.data.AUTOTUNE
    
    # load a single tf record element
    # INPUTS:
    #    element (bytes) - serialized bytes corresponding to a single tf record element
    # OUTPUTS:
    #    inputArr (tuple) - deep learning model inputs
    #    lab (np matrix) - one-hot encoded deep learning model labels
    def parse_tfr_element(self,element):
        # tf record structure
        data = {
            'input_ids':tf.io.FixedLenFeature([],tf.string),
            'masks':tf.io.FixedLenFeature([],tf.string),
            'labels':tf.io.FixedLenFeature([],tf.string),
            'vectors':tf.io.FixedLenFeature([],tf.string),
            'user_stats':tf.io.FixedLenFeature([],tf.string),
        }

        # parse serialized bytes to get records. restructure content into inputs and outputs for model training/testing
        content = tf.io.parse_single_example(element, data)
        input_ids = content['input_ids']
        masks = content['masks']
        labels = content['labels']
        vectors = content['vectors']
        user_stats = content['user_stats']
        in_ids = tf.io.parse_tensor(input_ids, out_type=tf.int32)
        msk = tf.io.parse_tensor(masks, out_type=tf.int32)
        lab = tf.io.parse_tensor(labels, out_type=tf.int64)
        inputArr  = (in_ids,msk)

        # if the model will use author vectors as model inputs, add author vectors to the input tuple
        if(self.vector):
            vec = tf.io.parse_tensor(vectors, out_type=tf.float32)
            inputArr = (*inputArr,vec)
        
        # if the model will user author statistics as model inputs, add author stats to the input tuple
        if(self.userStats):
            user_stats = tf.cast(tf.io.parse_tensor(user_stats, out_type=tf.float64),tf.float32)
            inputArr = (*inputArr,user_stats)
        return([inputArr,lab])
    
            
    # load tf records into memory
    # INPUTS:
    #    tfdir (str) - tf record folderpath
    #    pattern (str) - search pattern that defines tf record files
    def load_dataset(self,tfdir,pattern):
        print("loading tf records from directory %s" %(tfdir))
        fileList =  list(glob.glob(tfdir+pattern,recursive=False))
        np.random.shuffle(fileList)
        print("found %i tf records \n\n\n" %(len(fileList)))

        # create tf record data loader
        dataset = tf.data.TFRecordDataset(fileList)
        ignore_order = tf.data.Options()
        ignore_order.experimental_deterministic = False  # disable order, increase speed

        dataset.with_options(ignore_order)  # uses data as soon as it streams in, rather than in its original order
        dataset = dataset.map(self.parse_tfr_element,num_parallel_calls=tf.data.AUTOTUNE)
        print("parsed tf records")
        dataset = dataset.shuffle(4096*10)
        return (dataset)
    
    # end of TFRecordRead_Class.py