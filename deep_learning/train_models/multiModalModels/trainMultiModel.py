import pandas as ps
import tensorflow as tf
import os
import glob
import numpy as np
from transformers import BertTokenizer, TFBertForMaskedLM
from keras.callbacks import ModelCheckpoint
from transformers import *
from mySecrets import secrets

def createLabel(curRecord):
    return([
        curRecord['isBaby'],
        curRecord['isToddler'],
        curRecord['isElem'],
        curRecord['isMiddle'],
        curRecord['isHigh'],
        curRecord['isChild']
    ])

def createHybridModel():
    inputVals = tf.keras.layers.Input((14), dtype=tf.float32, name='inputs')
    hiddenLayer = tf.keras.layers.Dense(1024,activation='relu',name='hidden')(inputVals)
    hiddenLayer = tf.keras.layers.Dropout(0.2)(hiddenLayer)
    hiddenLayer = tf.keras.layers.Dense(128,activation='relu')(hiddenLayer)
    predLayer = tf.keras.layers.Dense(6,activation=None,name='categorical')(hiddenLayer)
    model = tf.keras.Model(inputs = inputVals,outputs = predLayer)
    return(model)

def loadDataset(textFilepath,textModelPreds,imageModelPreds,recordsWithImgs):
    testText = ps.read_csv(textFilepath,encoding='utf-8',lineterminator='\n')
    testText['imgKey'] = testText['img_http'].apply(lambda x: x[:-4])
    testText['textIndex'] = range(testText.count().iloc[0])
    joined = testText.merge(recordsWithImgs,how='inner',on='imgKey')
    joined.drop_duplicates(inplace=True)
    faceProbs = np.load(imageModelPreds)
    textProbs = np.load(textModelPreds)
    testHybridRecords,testLabels = [],[]
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

def runModel(model,trainHybridInputs,trainHybridLabels,testHybridInputs,testHybridLabels):
    
    model.compile(loss=[tf.keras.losses.BinaryCrossentropy(from_logits=True)],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001))
    
    checkpoint = ModelCheckpoint(filepath=secrets['HYBRID_MODEL_CHECKPOINT'],
                                monitor='val_loss',
                                save_best_only=True,
                                save_weights_only=False,
                                verbose=0,
                                initial_value_threshold=  None
    )
                                
    model.fit(trainHybridInputs,trainHybridLabels,
                            batch_size=512,
                            validation_data=(testHybridInputs,testHybridLabels),
                            epochs=5000,
                            callbacks=[checkpoint],
                            shuffle=True   
    )

recordsWithImgs = ps.read_csv(secrets['RECORDS_WITH_IMGS_FILEPATH'])
recordsWithImgs['imgKey'] = recordsWithImgs['imgId']
recordsWithImgs['imgIndex'] = range(recordsWithImgs.count()[0])
recordsWithImgs = recordsWithImgs[['imgKey','imgIndex']]

testInputs,trainlabels = loadDataset(secrets['TEST_TEXT_FILEPATH'],secrets['TEST_TEXT_PREDICTIONS'],secrets['TEST_IMAGE_PREDICTIONS'],recordsWithImgs)
trainInputs,trainLabels = loadDataset(secrets['TRAIN_TEXT_FILEPATH'],secrets['TRAIN_TEXT_PREDICTIONS'],secrets['TRAIN_IMAGE_PREDICTIONS'],recordsWithImgs)

with tf.device('/GPU:0'):
    hybridModel = createHybridModel()
    runModel(hybridModel)

