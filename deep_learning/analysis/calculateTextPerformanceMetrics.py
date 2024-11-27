import pandas as ps
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
import transformers

# custom classes
from TFRecordRead_Class import TFRecordRead
from mySecrets import secrets

def loadDataset(dataFolder):
    recordReader = TFRecordRead(dataFolder,128,2)
    dataset = recordReader.tfRecords.prefetch(buffer_size=AUTOTUNE)
    dataset = dataset.batch(128)
    print("loaded dataset")
    return(dataset)

print("Num GPUs Available: ", len(tf.config.list_physical_devices('GPU')))
print(tf.config.list_physical_devices('GPU'))
AUTOTUNE = tf.data.experimental.AUTOTUNE
TRAIN_BATCH_SIZE = 128
VAL_BATCH_SIZE = 128
testData = loadDataset(secrets['TEST_DATA_FOLDER'])

def getLabels():
    labelSet = []
    for images, labels in testData.take(1000):
        if(len(labelSet)==0):
            labelSet = labels.numpy()
        else:
            labelSet = np.concatenate((labelSet,labels))
    print(labelSet.shape)
    return(labelSet)

def getPredictions(dataset,model):
    labelSet,predictions = [],[]
    for text, labels in testData.take(1000):
        print(text)
        pred = model.predict(text)
        if(len(labelSet)==0):
            labelSet = labels.numpy()
            predictions = pred
        else:
            labelSet = np.concatenate((labelSet,labels))
            predictions = np.concatenate((predictions,pred))
    return([labelSet,predictions])


def calcTruePositive(labels,predictions,debug=False):
    correct = np.multiply(labels,predictions)
    nCorrect = np.sum(correct,axis=0)
    if(debug):
        percCorrect = nCorrect/np.sum(labels,axis=0)*100
        return([nCorrect,percCorrect])
    return(nCorrect)

def calcTrueNegative(labels,predictions,debug=False):
    labZeros = np.where(labels==0,1,0)
    predZeros = np.where(predictions==0,1,0)
    correct = np.multiply(labZeros,predZeros)
    nCorrect = np.sum(correct,axis=0)
    if debug:
        percCorrect = nCorrect/np.sum(labZeros,axis=0)*100
        return([nCorrect,percCorrect])
    return(nCorrect)

def calcFalseNegative(labels,predictions,debug=False):
    predZeros = np.where(predictions==0,1,0)
    correct = np.multiply(labels,predZeros)
    nCorrect = np.sum(correct,axis=0)
    if debug:
        percCorrect = nCorrect/np.sum(labels,axis=0)*100
        return([nCorrect,percCorrect])
    return(nCorrect)

def calcFalsePositive(labels,predictions,debug=False):
    labZeros = np.where(labels==0,1,0)
    correct = np.multiply(labZeros,predictions)
    nCorrect = np.sum(correct,axis=0)
    if(debug):
        percCorrect = nCorrect/np.sum(labels,axis=0)*100
        return([nCorrect,percCorrect])
    return(nCorrect)

def calcConfusionMatrix(labels,predictions):
    predictionsRound = tf.round(tf.nn.sigmoid(predictions))
    confusionMatrix = ps.DataFrame({
        'truePositive':calcTruePositive(labels,predictionsRound).astype(int),
        'trueNegative':calcTrueNegative(labels,predictionsRound).astype(int),
        'falsePositive':calcFalsePositive(labels,predictionsRound).astype(int),
        'falseNegative':calcFalseNegative(labels,predictionsRound).astype(int)
    })
    return(confusionMatrix)

predModel = tf.keras.models.load_model(secrets['TEXT_NO_VECTOR_MODEL_FILEPATH'],custom_objects={"TFBertModel": transformers.TFBertModel})
predModel.compile(loss=[tf.keras.losses.BinaryCrossentropy()],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])
labels,predictions = getPredictions(testData,predModel)
calcConfusionMatrix(labels,predictions)