# calculateTextPerformanceMetrics.py #
# Author: Andrew Larkin
# Date Created: November 27th, 2024

# Summary: given validation datasets, calculate a confusion matrix for one of the 
#          Aspire social media text deep learning models 

# import libraries
import pandas as ps
import numpy as np
import matplotlib.pyplot as plt
import tensorflow as tf
import transformers

# import custom classes
from TFRecordRead_Class import TFRecordRead
from mySecrets import secrets


# global constants
AUTOTUNE = tf.data.experimental.AUTOTUNE
TRAIN_BATCH_SIZE = 128
VAL_BATCH_SIZE = 128
VALIDATION_SIZE = 5000 # number of records in validation dataset

# use custom class to load TF records 
# INPUTS:
#    dataFolder (str) - folderpath where tf records are stored
# OUTPUTS:
#    dataset (tf record reader)
def loadDataset(dataFolder):
    recordReader = TFRecordRead(dataFolder,128,2)
    dataset = recordReader.tfRecords.prefetch(buffer_size=AUTOTUNE)
    dataset = dataset.batch(128)
    print("loaded dataset")
    return(dataset)

# predict outputs for validation dataset
# INPUTS:
#    dataset (tf record reader) - object that will serve tf records to the model
#    model (tf model) - model that will generate predictions
# OUTPUTS:
#    labelSet (list of np int arrays) - multilabel classifications for validation dataset
#    predictions (list of np float arrays) - multilabel probabilities predicted by model
def getPredictions(dataset,model):
    labelSet,predictions = [],[]
    for text, labels in testData.take(VALIDATION_SIZE):
        print(text)
        pred = model.predict(text)
        if(len(labelSet)==0):
            labelSet = labels.numpy()
            predictions = pred
        else:
            labelSet = np.concatenate((labelSet,labels))
            predictions = np.concatenate((predictions,pred))
    return([labelSet,predictions])

# calculate the number of true positive examples predicted by the model
# INPUTS:
#    labels (list of np int arrays) - multilabel classifications
#    predictions (list of np int arrays) - multilabel predictions
#    debug (boolean) - whether to execute branching debug commands
# OUTPUTS:
#    nCorrect (int) - number of true positive predictions
def calcTruePositive(labels,predictions,debug=False):
    correct = np.multiply(labels,predictions)
    nCorrect = np.sum(correct,axis=0)
    if(debug):
        percCorrect = nCorrect/np.sum(labels,axis=0)*100
        return([nCorrect,percCorrect])
    return(nCorrect)

# calculate the number of true negative examples predicted by the model
# INPUTS:
#    labels (list of np int arrays) - multilabel classifications
#    predictions (list of np int arrays) - multilabel predictions
#    debug (boolean) - whether to execute branching debug commands
# OUTPUTS:
#    nCorrect (int) - number of true negative predictions
def calcTrueNegative(labels,predictions,debug=False):
    labZeros = np.where(labels==0,1,0)
    predZeros = np.where(predictions==0,1,0)
    correct = np.multiply(labZeros,predZeros)
    nCorrect = np.sum(correct,axis=0)
    if debug:
        percCorrect = nCorrect/np.sum(labZeros,axis=0)*100
        return([nCorrect,percCorrect])
    return(nCorrect)

# calculate the number of false negative examples predicted by the model
# INPUTS:
#    labels (list of np int arrays) - multilabel classifications
#    predictions (list of np int arrays) - multilabel predictions
#    debug (boolean) - whether to execute branching debug commands
# OUTPUTS:
#    nIncorrect (int) - number of false negative predictions
def calcFalseNegative(labels,predictions,debug=False):
    predZeros = np.where(predictions==0,1,0)
    correct = np.multiply(labels,predZeros)
    nIncorrect = np.sum(correct,axis=0)
    if debug:
        percCorrect = nIncorrect/np.sum(labels,axis=0)*100
        return([nIncorrect,percCorrect])
    return(nIncorrect)

# calculate the number of false positive examples predicted by the model
# INPUTS:
#    labels (list of np int arrays) - multilabel classifications
#    predictions (list of np int arrays) - multilabel predictions
#    debug (boolean) - whether to execute branching debug commands
# OUTPUTS:
#    nIncorrect (int) - number of false positive predictions
def calcFalsePositive(labels,predictions,debug=False):
    labZeros = np.where(labels==0,1,0)
    correct = np.multiply(labZeros,predictions)
    nIncorrect = np.sum(correct,axis=0)
    if(debug):
        percCorrect = nIncorrect/np.sum(labels,axis=0)*100
        return([nIncorrect,percCorrect])
    return(nIncorrect)

# calculate confusion matrix for a set of predictions 
# INPUTS:
#    labels (list of np int arrays) - multilabel classifications
#    predictions (list of np int arrays) - multilabel predictions
# OUTPUTS:
#    confusion matrix as a pandas dataframe
def calcConfusionMatrix(labels,predictions):
    predictionsRound = tf.round(tf.nn.sigmoid(predictions))
    confusionMatrix = ps.DataFrame({
        'truePositive':calcTruePositive(labels,predictionsRound).astype(int),
        'trueNegative':calcTrueNegative(labels,predictionsRound).astype(int),
        'falsePositive':calcFalsePositive(labels,predictionsRound).astype(int),
        'falseNegative':calcFalseNegative(labels,predictionsRound).astype(int)
    })
    return(confusionMatrix)

if __name__ == "__main__":
    
    # test if gpus are available. If GPUs aren't available, the validation dataset can still be evaluated 
    # but generating predictions may take several minutes
    print("Num GPUs Available: ", len(tf.config.list_physical_devices('GPU')))
    print(tf.config.list_physical_devices('GPU'))
    
    # load validation datset 
    testData = loadDataset(secrets['TEST_DATA_FOLDER'])

    # load deep learning model
    predModel = tf.keras.models.load_model(secrets['TEXT_NO_VECTOR_MODEL_FILEPATH'],custom_objects={"TFBertModel": transformers.TFBertModel})
    predModel.compile(loss=[tf.keras.losses.BinaryCrossentropy()],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])

    # generate predictions from validation dataset
    labels,predictions = getPredictions(testData,predModel)

    # calculate confusion matrix for validation dataset
    calcConfusionMatrix(labels,predictions)