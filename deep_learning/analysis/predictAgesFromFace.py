### predictAgesFromFace.py ###
### Author: Andrew Larkin ###

### Summary: predict ages of children in social media imagery

# Import dependencies
import pandas as ps
import numpy as np
import os
import tensorflow as tf
import glob

# load secrets
from mySecrets import secrets

# custom loss function used to train age image deep learning model
class CustomAccuracy(tf.keras.losses.Loss):
        def __init__(self):
            super().__init__()
        def call(self, y_true, y_pred):

            # predict if there is any child in the image
            y_pred2 = tf.argmax(y_pred)
            # look at labels to determine is there is any child in the image
            y_true2 = tf.argmax(y_true)
            diff = tf.math.abs(tf.math.subtract(y_pred2,y_true2))

            # if the model or label are either positive for there being a child in the image,
            # set the potential loss as 1.5 instead of 1 to make this mislcassification more 
            # heavily weighted
            maxLoss = tf.where(tf.math.equal(y_pred2,0),1.5,0)
            maxLoss2 = tf.where(tf.math.equal(y_true2,0),1.5,0)          
            maxLoss = tf.math.maximum(maxLoss,maxLoss2)

            # calcualte the loss for mispredicting if a child is present in the image
            loss1 = tf.cast(tf.math.reduce_mean(tf.math.minimum(tf.cast(diff,tf.float32),maxLoss),axis=None),tf.float32)

            # calcualte the losses for each age group classification
            cce = tf.keras.losses.CategoricalCrossentropy()
            loss2 = cce(y_true,y_pred)

            # return the sum of 2 losses
            return (loss2+loss1)


# load image and convert to greyscale
# INPUTS:
#    filename (str) - absolute image filepath
# OUTPUTS:
#    image_decoded (numpy array) - 2x2 np matrix
def _parse_function(filename):   
    image_string = tf.io.read_file(filename)
    image_decoded = tf.io.decode_jpeg(image_string, channels=1)    # channels=1 to convert to grayscale, channels=3 to convert to RGB.
    return image_decoded

# get list of images to predict ages from 
# INPUTS:
#    inFolder (str) - folder filepath
# OUTPUTS:
#    filePaths (str list) - absolute filepaths to images
def getImageList(inFolder):
    filepaths = glob.glob(inFolder + "*.jpg")
    print("found %i image files to read" %(len(filepaths)))
    return(filepaths)

# get unique identifier for social media image
# (images often have more than 1 face, thus multiple face clips can have the same
#  social media image id)
# INPUTS:
#    filepath (str) - absolute filepath to temporary image of clipped face
# OUTPUTS:
#    idSeq (str) - unique identifier for clipped face imageoutlo
def getIdFromFilepath(filepath):
     startIndex = filepath.rfind('/') +1
     endIndex = filepath.find('_index')
     idSeq = filepath[startIndex:endIndex]
     return(idSeq)

# remove face clip records where the model predicted the image is not a person
# (i.e. maxPred = 0)
# INPUTS:
#    df (pandas df) - contains predicted probabilities for each face clip image
# OUTPUTS:
#    input dataframe restricted to records where face clip records where predicted 
#    to be human faces
def screenArgMax(df):
     screenedDF = df[df['maxPred']>0]
     return(screenedDF)

# for all face clips within a social media image, get the maximum predicted probability
# for each age group category
# INPUTS:
#    seqId (str) - unique identifier of the social meddia image
# OUTPUTS:
#    maximum probability for each age group cateogry
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

if __name__ == "__main__":

    # Testing to ensure GPU is being utilized. If GPU is not available
    # then generating predictions will take several minutes
    device_name = tf.test.gpu_device_name()
    if device_name != '/device:GPU:0':
        raise SystemError('GPU device not found')
    print('Found GPU at: {}'.format(device_name))

    # get list of face clips to predict age groups for and load into a data server
    imgList = getImageList(secrets['IMAGE_FOLDER'])
    predDataTensor = tf.constant(imgList)
    predDataset = tf.data.Dataset.from_tensor_slices((predDataTensor))
    predDataset = predDataset.map(_parse_function)
    predDataset = predDataset.batch(512)

    # load prediction model weights
    modelFile = secrets['MODEL_FILEPATH']
    final_cnn = tf.keras.models.load_model(modelFile,compile=False)
    final_cnn.compile(loss=[CustomAccuracy()],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])

    # for each face clip predict the age group. Save predictions to csv and np array
    final_cnn_pred = final_cnn.predict(predDataset)
    imgDF = ps.DataFrame({
        'imgName':imgList,
    })
    imgDF.to_csv(secrets['PER_FACE_CLASSIFICATIONS'],index=False)
    np.save(secrets['PER_FACE_PROBABILITIES'], final_cnn_pred)

    # remove face clip records where the model predicted the face clip is not human
    # (category 0)
    imgDF['rowIndex'] = range(0, imgDF.count()[0])
    imgDF['maxPred'] = final_cnn_pred.argmax(axis=-1)
    imgDF = screenArgMax(imgDF)

    # map face clip predictions to original social media images
    imgList = list(imgDF['imgName'])
    idSeq = list(map(getIdFromFilepath,imgList))
    imgDF['idSeq'] = idSeq
    uniqueIds = list(set(idSeq))

    # for each social media image, get the predited probability for each age group classificaiton
    # and save to csv and np arrays
    maxProbs = list(map(getMaxProbs,uniqueIds))
    df = ps.DataFrame({
        'imgId':uniqueIds
    })
    df.to_csv(secrets['PER_IMAGE_CLASSIFICATIONS'],index=False)
    np.save(secrets['PER_IMAGE_PROBABILITIES'],maxProbs)

# end of predictAgesFromFace.py