import tensorflow as tf
import os
import glob
import numpy as np


class TFRecordRead():
    
    def __init__(self,dataFolder,batchSize,vector=False,userStats=False):
        self.dataFolder = dataFolder
        self.vector = vector
        self.userStats = userStats
        self.tfRecords = self.load_dataset(dataFolder,'*.tfrecords')
        self.batch_size = batchSize
        self.AUTOTUNE = tf.data.AUTOTUNE
    
        
    def parse_tfr_element(self,element):
      #use the same structure as above; it's kinda an outline of the structure we now want to create
        data = {
            'input_ids':tf.io.FixedLenFeature([],tf.string),
            'masks':tf.io.FixedLenFeature([],tf.string),
            'labels':tf.io.FixedLenFeature([],tf.string),
            'vectors':tf.io.FixedLenFeature([],tf.string),
            'user_stats':tf.io.FixedLenFeature([],tf.string),
        }


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
        if(self.vector):
            vec = tf.io.parse_tensor(vectors, out_type=tf.float32)
            inputArr = (*inputArr,vec)
        if(self.userStats):
            user_stats = tf.cast(tf.io.parse_tensor(user_stats, out_type=tf.float64),tf.float32)
            inputArr = (*inputArr,user_stats)
        return([inputArr,lab])
    
            

    def load_dataset(self,tfdir,pattern):
        print(tfdir)
        print(len(os.listdir(tfdir)))
        files =  glob.glob(tfdir+pattern,recursive=False)
        a = list(files)
        np.random.shuffle(a)
        print("found %i tf records \n\n\n" %(len(a)))
        dataset = tf.data.TFRecordDataset(a)
        ignore_order = tf.data.Options()
        ignore_order.experimental_deterministic = False  # disable order, increase speed

        dataset.with_options(ignore_order)  # uses data as soon as it streams in, rather than in its original order
        dataset = dataset.map(self.parse_tfr_element,num_parallel_calls=tf.data.AUTOTUNE)
        print("parsed tf records")
        dataset = dataset.shuffle(4096*10)
        return (dataset)
    