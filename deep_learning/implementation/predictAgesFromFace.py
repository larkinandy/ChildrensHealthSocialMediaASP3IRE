# predictAgesFromFace.py 
# Author: Andrew Larkin

# Summary: Use the image-based deep learning model to predict 
#          child development stage from a social media image

# Import libraries
import pandas as ps
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
import glob

# import custom classes and secrets
from mySecrets import secrets

# Testing to ensure GPU is being utilized
device_name = tf.test.gpu_device_name()
if device_name != '/device:GPU:0':
    raise SystemError('GPU device not found')
print('Found GPU at: {}'.format(device_name))

# load image from file and convert to grayscale
# INPUTS:
#    filename (str) - absolute filepath to image
# OUTPUTS:
#    image_decoded (tensor) - image values stored as a tf tensor
def _parse_function(filename):   
    image_string = tf.io.read_file(filename)
    image_decoded = tf.io.decode_jpeg(image_string, channels=1)    # channels=1 to convert to grayscale, channels=3 to convert to RGB.
    # image_resized = tf.image.resize(image_decoded, [200, 200])
    return image_decoded

# get list of social media images to predict age groups for
# INPUTS:
#    inFolder (str) - folderath where images are stored in .jpg format
# OUTPUTS:
#    str list of absolute image filepaths
def getImageList(inFolder):
    filepaths = glob.glob(inFolder + "*.jpg")
    print("found %i image files to read" %(len(filepaths)))
    return(filepaths)

# extract image id from filename
# INPUTS:
#    filepath (str) - absolute filepath to image
# OUTPUTS:
#    idSeq (str) - uniue id for image
def getIdFromFilepath(filepath):
     startIndex = filepath.rfind('/') +1
     endIndex = filepath.find('_index')
     idSeq = filepath[startIndex:endIndex]
     return(idSeq)

# remove predictions from the model that equal 0 (i.e. no faces detected 
# in the image to predict age)
# INPUTS:
#    df (pandas dataframe) - contains model predictions
# OUTPUTS:
#    input dataframe, screened to remove images with no faces detected
def screenArgMax(df):
     screenedDF = df[df['maxPred']>0]
     return(screenedDF)

# for images with multiple faces detected, combine predictions using the max arg
# INPUTS:
#    seqId (str) - unique id for each social media image
# OUTPUTS:
#    newArr (np float) - combined predictions 
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
     
# create custom loss metric that includes a loss value for whether the image 
# correcctly identifies that there is a child (any age group) present in the image
class CustomAccuracy(tf.keras.losses.Loss):
        def __init__(self):
            super().__init__()
        def call(self, y_true, y_pred):
            y_pred2 = tf.argmax(y_pred)
            y_true2 = tf.argmax(y_true)
            diff = tf.math.abs(tf.math.subtract(y_pred2,y_true2))
            maxLoss = tf.where(tf.math.equal(y_pred2,0),1.5,0)
            maxLoss2 = tf.where(tf.math.equal(y_true2,0),1.5,0)
            
            maxLoss = tf.math.maximum(maxLoss,maxLoss2)
            loss1 = tf.cast(tf.math.reduce_mean(tf.math.minimum(tf.cast(diff,tf.float32),maxLoss),axis=None),tf.float32)
            cce = tf.keras.losses.CategoricalCrossentropy()
            loss2 = cce(y_true,y_pred)
            return (loss2+loss1)


# get absoulute filepaths of images to predict age groups for
imgList = getImageList(secrets['IMAGE_FOLDER'])
predDataTensor = tf.constant(imgList)
predDataset = tf.data.Dataset.from_tensor_slices((predDataTensor))
predDataset = predDataset.map(_parse_function)
predDataset = predDataset.batch(512)

# compile image model and load weights
final_cnn = tf.keras.models.load_model(secrets['MODEL_WEIGHTS_FILEPATH'],custom_objects={'CustomAccuracy':CustomAccuracy},compile=False)
final_cnn.compile(loss=[CustomAccuracy()],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])

# predict child age group probabilities for each face in all of the images
final_cnn_pred = final_cnn.predict(predDataset)

# predict child age group classifications for each face in all of the images
final_cnn_pred2 = final_cnn_pred.argmax(axis=-1)

# save per face model predictions to disk
imgDF = ps.DataFrame({
     'imgName':imgList,
})
imgDF.to_csv(secrets['PRED_FOLDER'] + 'perFacePred.csv',index=False)
np.save(secrets['PRED_FOLDER'] + 'perFaceProbs.npy', final_cnn_pred)

# add per face predictions to pandas df
imgDF['rowIndex'] = range(0, imgDF.count()[0])
imgDF['maxPred'] = final_cnn_pred2
imgDF = screenArgMax(imgDF)

# combine per face predictions to create predictions for each social media image
imgList = list(imgDF['imgName'])
idSeq = list(map(getIdFromFilepath,imgList))
imgDF['idSeq'] = idSeq
uniqueIds = list(set(idSeq))
maxProbs = list(map(getMaxProbs,uniqueIds))
df = ps.DataFrame({
     'imgId':uniqueIds
})

# save per image probabilities and classifications to disk
df.to_csv(secrets['PRED_FOLDER'] + 'perImgPred.csv',index=False)
np.save(secrets['PRED_FOLDER'] + 'perImgProbs.npy',maxProbs)

# end of predictAgesFromFace.py