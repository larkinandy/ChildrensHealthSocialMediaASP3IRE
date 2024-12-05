# trainTextModel.py
# Author: Andrew Larkin

# Summary: train a model to predict age,place,or health classifications from social media text

# import libraries
import tensorflow as tf
from transformers import BertTokenizer, TFBertForMaskedLM
from keras.callbacks import ModelCheckpoint
from transformers import *
import keras
import sys

# import custom classes and secrets
from TFRecordRead_Class import TFRecordRead
from mySecrets import secrets
    

# load tf records into memory
#    dataFolder (str) - absolute folderpath where tf records are stored
#    vector (Boolean) - whether to include user summary vectors in model
#    userStats (Boolean) - whether to include user statistics in model
# OUTPUTS:
#    TFRecordRead object for serving records to a tf model
def loadDataset(dataFolder,vector=False,userStats=False):
    recordReader = TFRecordRead(dataFolder,BATCH_SIZE,vector,userStats)
    dataset = recordReader.tfRecords.prefetch(buffer_size=AUTOTUNE)
    dataset = dataset.batch(BATCH_SIZE,deterministic=False)
    print("loaded dataset")
    return(dataset)

# build classification model architecture 
# INPUTS:
#    vector (Boolean) - whether to include user summary vectors in model architecture
#    userStats (Boolean) - whether to include user statistics in model architecture
#    fixWeights (Boolean) - whether to fix the BERT layer weights during trianing
#    nOutputs (int) - number of multiclasses to predict
# OUTPUTS:
#    bm2 (tf keras model) - model arcchitecture
def buildClassifictionModel(vector=False,userStats=False,fixWeights=False,nOutputs=6):
    MAX_SEQ_LEN = 100 # max number of words for a sentence 

    # define inputs 
    t_i = tf.keras.layers.Input((MAX_SEQ_LEN), dtype=tf.int32, name='input_ids')
    m_i = tf.keras.layers.Input((MAX_SEQ_LEN,), dtype=tf.int32, name='atttention_mask')
    inVals = [t_i,m_i]
    if(vector):
        v_i = tf.keras.layers.Input((VECTOR_SIZE), dtype=tf.float32, name='vector')
        inVals.append(v_i)
    if(userStats):
        u_i = tf.keras.layers.Input((USER_STATS_SIZE), dtype=tf.float32, name='user_stats')
        inVals.append(u_i)

    # load BERT weights into first few layers of the model
    btok = TFBertModel.from_pretrained(secrets['PRETRAINED_BERT_BASE_FILEPATH'])

    #experimental block for freezing BERT weights. Did not improve model performance
    if(fixWeights):
        trainableWeights = [
            'tf_bert_model/bert/pooler/dense/kernel:0',
            'tf_bert_model/bert/pooler/dense/bias:0'
    ]
        for w in btok.bert.weights:
            if(w.name in trainableWeights):
                w._trainable = True
            else:   
                for layer in list(range(10,12)):
                    if(str(layer) in w.name):
                        w._trainable = True
                        break
                else:
                    w._trainable=False
    bertOutput = btok([t_i,m_i])
    combinedOutput = (bertOutput['pooler_output'])

    # add vector and user stats if input args were True
    if(vector):
        combinedOutput = tf.concat([combinedOutput,v_i],-1)
    if(userStats):
        combinedOutput = tf.concat([combinedOutput,u_i],-1)
    combinedOutput = tf.keras.layers.Dropout(0.2)(combinedOutput)

    # fully connected layers before final output
    newLayers = tf.keras.layers.Dense(128,activation='relu',name='activate1')(combinedOutput)
    newLayers = tf.keras.layers.Dropout(0.2)(newLayers)
    newLayers = tf.keras.layers.Dense(128,activation='relu',name='activate2')(newLayers)
    newLayers = tf.keras.layers.Dropout(0.2)(newLayers)
    final = tf.keras.layers.Dense(nOutputs,activation=None,name='categorical')(newLayers)
    bm2 = tf.keras.Model(inputs = [inVals],outputs = final)
    return(bm2)

# train the classification model
# INPUTS:
#    model (tf keras model) - contains model architecture
def trainModel(model):
    model.compile(
        loss=[tf.keras.losses.BinaryCrossentropy(from_logits=True)],
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001)
    )
    
    # define conditions for saving model weights
    checkpoint = ModelCheckpoint(
        filepath=PARENT_FOLDER + secrets['MODEL_CHECKPOINT_FILEPATH'],
        monitor='val_loss',
        save_best_only=True,
        save_weights_only=False,
        verbose=1,
        initial_value_threshold=  None
    )
                                
    # train model                                
    model.fit(
        trainData,
        batch_size=BATCH_SIZE,
        validation_data=testData,
        epochs=1000,
        callbacks=[checkpoint],
        shuffle=True    # shuffle=False to reduce randomness and increase reproducibility
    )

# main function
if __name__ == '__main__':

    print("Num GPUs Available: ", len(tf.config.list_physical_devices('GPU')))
    print(tf.config.list_physical_devices('GPU'))
    AUTOTUNE = tf.data.experimental.AUTOTUNE

    # define global constants
    BATCH_SIZE = 128
    MAX_SEQ_LEN = 100
    VECTOR_SIZE = 512
    USER_STATS_SIZE = 6

    # load runtime arguments
    USE_VECTOR = sys.argv[1] # Boolean, whether to use user vector inputs
    USE_STATS = sys.argv[2] # Boolean, whether to use user stats inputs
    modelType = sys.argv[3] # string, either 'age','place', or 'health'

    # set model type depending on which labels a model should be trained to predict
    if(modelType=='age'):
        PARENT_FOLDER = secrets['CHILD_FOLDER']
    elif(modelType=='place'):
        PARENT_FOLDER = secrets['PLACE_FOLDER']
    else:
        PARENT_FOLDER = secrets['HEALTH_FOLDER']

    # load training and test datasets
    trainData = loadDataset(PARENT_FOLDER + secrets['TRAIN_DATA_SUBFOLDER'],USE_VECTOR,USE_STATS)
    testData = loadDataset(PARENT_FOLDER + secrets['TEST_DATA_SUBFOLDER'],USE_VECTOR,USE_STATS)

    with tf.device('/GPU:0'):
        # build model architecture
        classificationModel = buildClassifictionModel(USE_VECTOR,USE_STATS)
        print(classificationModel.summary())

        # train the model
        trainModel(classificationModel)

# end of trainTextModel.py