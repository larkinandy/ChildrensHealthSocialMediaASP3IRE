# trainMultiModel.py
# Author: Andrew Larkin

# Summary: train a model that uses both text and imagery to predict child age groups

# import libraries
import pandas as ps
import tensorflow as tf
import os
import glob
import numpy as np
from transformers import BertTokenizer, TFBertForMaskedLM
from keras.callbacks import ModelCheckpoint
from transformers import *

# import secrets
from mySecrets import secrets

# create multi class encoding label
# INPUTS:
#    curRecord (pandas dataframe) - contains one record (row)
# OUTPUTS:
#    np binary array
def createLabel(curRecord):
    return([
        curRecord['isBaby'],
        curRecord['isToddler'],
        curRecord['isElem'],
        curRecord['isMiddle'],
        curRecord['isHigh'],
        curRecord['isChild']
    ])

# define model architecture
# OUTPUTS:
#    model (keras model) - contains model architecture
def createHybridModel():
    inputVals = tf.keras.layers.Input((14), dtype=tf.float32, name='inputs') # 7 inputs from text model, 7 from image model
    hiddenLayer = tf.keras.layers.Dense(1024,activation='relu',name='hidden')(inputVals)
    hiddenLayer = tf.keras.layers.Dropout(0.2)(hiddenLayer)
    hiddenLayer = tf.keras.layers.Dense(128,activation='relu')(hiddenLayer)
    predLayer = tf.keras.layers.Dense(6,activation=None,name='categorical')(hiddenLayer)
    model = tf.keras.Model(inputs = inputVals,outputs = predLayer)
    return(model)

# combine text and image model predictions to create a multimodal input dataset
# INPUTS:
#    textFilepath (str) - absolute filepath to social media posts with text
#    textModelPreds (str) - absolute filepath to text model predictions
#    imageModelPreds (str) - absolute filepath to image model predictions
#    recordsWithImgs (pandas dataframe) - social media posts with imagery
# OUTPUTS:
#    testInputs (np matrix) - inputs for the multimodal model
#    testLabels (np matrix) - labels for the multimodal model
def loadDataset(textFilepath,textModelPreds,imageModelPreds,recordsWithImgs):

    # load text social meida posts and inner join with those that also have imagery
    testText = ps.read_csv(textFilepath,encoding='utf-8',lineterminator='\n')
    testText['imgKey'] = testText['img_http'].apply(lambda x: x[:-4])
    testText['textIndex'] = range(testText.count().iloc[0])
    joined = testText.merge(recordsWithImgs,how='inner',on='imgKey')
    joined.drop_duplicates(inplace=True)

    # load text and image model predictions
    faceProbs = np.load(imageModelPreds)
    textProbs = np.load(textModelPreds)
    testHybridRecords,testLabels = [],[]

    # combine text and image model predictions to create single input into 
    # multimodal input
    for recordNum in range(joined.count().iloc[0]):
        curRecord = joined.iloc[recordNum]
        curImgIndex = curRecord['imgIndex']
        curTextIndex = curRecord['textIndex']
        curImgProb = faceProbs[curImgIndex]
        curTextProb = textProbs[curTextIndex]
        hybridRecord = np.concatenate((curImgProb,curTextProb),axis=-1)
        testHybridRecords.append(hybridRecord)
        testLabels.append(createLabel(curRecord))
    testInputs = np.stack(testHybridRecords)
    testLabels = np.stack(testLabels)
    return(testInputs,testLabels)

# train a multimodal model 
# INPUTS:
#    model (keras model) - contains model architecture
#    trainHybridInputs (np matrix) - inputs to train model
#    trainHybridLabels (np matrix) - labels to train model
#    testHybridInputs (np matrix) - inputs to test model
#    testHybridLabels (np matrix) - labels to test model
def trainModel(model,trainHybridInputs,trainHybridLabels,testHybridInputs,testHybridLabels):
    
    # compile model
    model.compile(loss=[tf.keras.losses.BinaryCrossentropy(from_logits=True)],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001))
    
    # define conditions for saving model weights
    checkpoint = ModelCheckpoint(
        filepath=secrets['HYBRID_MODEL_CHECKPOINT'],
        monitor='val_loss',
        save_best_only=True,
        save_weights_only=False,
        verbose=0,
        initial_value_threshold=  None
    )
                                
    # train model
    model.fit(
        trainHybridInputs,
        trainHybridLabels,
        batch_size=512,
        validation_data=(testHybridInputs,testHybridLabels),
        epochs=5000,
        callbacks=[checkpoint],
        shuffle=True   
    )

# main function
if __name__ == '__main__':

    # load train and test datasets
    recordsWithImgs = ps.read_csv(secrets['RECORDS_WITH_IMGS_FILEPATH'])
    recordsWithImgs['imgKey'] = recordsWithImgs['imgId']
    recordsWithImgs['imgIndex'] = range(recordsWithImgs.count()[0])
    recordsWithImgs = recordsWithImgs[['imgKey','imgIndex']]

    testInputs,trainlabels = loadDataset(secrets['TEST_TEXT_FILEPATH'],secrets['TEST_TEXT_PREDICTIONS'],secrets['TEST_IMAGE_PREDICTIONS'],recordsWithImgs)
    trainInputs,trainLabels = loadDataset(secrets['TRAIN_TEXT_FILEPATH'],secrets['TRAIN_TEXT_PREDICTIONS'],secrets['TRAIN_IMAGE_PREDICTIONS'],recordsWithImgs)

    # train model
    with tf.device('/GPU:0'):
        hybridModel = createHybridModel()
        trainModel(hybridModel)

# end of trainMultiModel.py