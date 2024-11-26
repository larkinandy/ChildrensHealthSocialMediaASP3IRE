# Import dependencies
import pandas as ps
import numpy as np
import matplotlib.pyplot as plt
import cv2
import os
from zipfile import ZipFile
import time
from datetime import datetime
import itertools
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import Conv2D, AveragePooling2D, GlobalAveragePooling2D, Dropout
from tensorflow.keras import utils 
from tensorflow.keras.callbacks import TensorBoard, ModelCheckpoint
import glob

# Setting random seeds to reduce the amount of randomness in the neural net weights and results
# The results may still not be exactly reproducible
np.random.seed(42)
tf.random.set_seed(42)

PARENT_FOLDER = "/mnt/z/Aspire/Analyses/child/images/"
IMAGE_FOLDER = PARENT_FOLDER + "faceClip/"
PRED_FOLDER = PARENT_FOLDER + "predictions/"


# Testing to ensure GPU is being utilized
# Ensure that the Runtime Type for this notebook is set to GPU
# If a GPU device is not found, change the runtime type under:
# Runtime>> Change runtime type>> Hardware accelerator>> GPU
# and run the notebook from the beginning again.

device_name = tf.test.gpu_device_name()
if device_name != '/device:GPU:0':
    raise SystemError('GPU device not found')
print('Found GPU at: {}'.format(device_name))


def _parse_function(filename):   
    image_string = tf.io.read_file(filename)
    image_decoded = tf.io.decode_jpeg(image_string, channels=1)    # channels=1 to convert to grayscale, channels=3 to convert to RGB.
    # image_resized = tf.image.resize(image_decoded, [200, 200])
    return image_decoded


def getImageList(inFolder):
    filepaths = glob.glob(inFolder + "*.jpg")
    print("found %i image files to read" %(len(filepaths)))
    return(filepaths)


def getIdFromFilepath(filepath):
     startIndex = filepath.rfind('/') +1
     endIndex = filepath.find('_index')
     idSeq = filepath[startIndex:endIndex]
     return(idSeq)

def screenArgMax(df):
     screenedDF = df[df['maxPred']>0]
     return(screenedDF)

def getMaxProbs(seqId):
    seqSubset = imgDF[imgDF['idSeq'] == seqId]
    nRows = seqSubset.count()[0]
    rowIndexes = list(seqSubset['rowIndex'])
    probs = final_cnn_pred.take(rowIndexes,axis=0) 
    if(nRows>1):
        probs = np.amax(probs,axis=0)
    else:
        probs = probs[0]
    newArr = np.concatenate(([nRows*1.0],probs),axis=0)
    return(newArr)
     

class CustomAccuracy(tf.keras.losses.Loss):
        def __init__(self):
            super().__init__()
        def call(self, y_true, y_pred):
            y_pred2 = tf.argmax(y_pred)
            y_true2 = tf.argmax(y_true)
            diff = tf.math.abs(tf.math.subtract(y_pred2,y_true2))
            #diff = tf.math.multiply(diff,2)
            maxLoss = tf.where(tf.math.equal(y_pred2,0),1.5,0)
            maxLoss2 = tf.where(tf.math.equal(y_true2,0),1.5,0)
            
            maxLoss = tf.math.maximum(maxLoss,maxLoss2)
            loss1 = tf.cast(tf.math.reduce_mean(tf.math.minimum(tf.cast(diff,tf.float32),maxLoss),axis=None),tf.float32)
            cce = tf.keras.losses.CategoricalCrossentropy()
            loss2 = cce(y_true,y_pred)
            return (loss2+loss1)



imgList = getImageList(IMAGE_FOLDER)
predDataTensor = tf.constant(imgList)
predDataset = tf.data.Dataset.from_tensor_slices((predDataTensor))
predDataset = predDataset.map(_parse_function)
predDataset = predDataset.batch(512)


modelFile = '/mnt/h/Aspire/faces/age_model_checkpoint_hybrid.h5'
final_cnn = tf.keras.models.load_model(modelFile,custom_objects={'CustomAccuracy':CustomAccuracy},compile=False)
final_cnn.compile(loss=[CustomAccuracy()],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])
final_cnn_pred = final_cnn.predict(predDataset)
print(final_cnn_pred[0:10])
final_cnn_pred2 = final_cnn_pred.argmax(axis=-1)
print(final_cnn_pred2[0:10])
print(imgList[0:10])
imgDF = ps.DataFrame({
     'imgName':imgList,
})
imgDF.to_csv('/mnt/h/Aspire/faces/perFacePred.csv',index=False)
np.save('/mnt/h/Aspire/faces/perFaceProbs.npy', final_cnn_pred)

#imgDF = ps.read_csv('/mnt/h/Aspire/faces/perFacePred.csv')

#final_cnn_pred = np.load('/mnt/h/Aspire/faces/perFaceProbs.npy')


imgDF['rowIndex'] = range(0, imgDF.count()[0])
imgDF['maxPred'] = final_cnn_pred.argmax(axis=-1)
imgDF = screenArgMax(imgDF)


imgList = list(imgDF['imgName'])
idSeq = list(map(getIdFromFilepath,imgList))
imgDF['idSeq'] = idSeq
uniqueIds = list(set(idSeq))
maxProbs = list(map(getMaxProbs,uniqueIds))
df = ps.DataFrame({
     'imgId':uniqueIds
})
df.to_csv('/mnt/h/Aspire/faces/perImgPred.csv',index=False)
np.save('/mnt/h/Aspire/faces/perImgProbs.npy',maxProbs)
