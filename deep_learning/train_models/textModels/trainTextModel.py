import tensorflow as tf
from transformers import BertTokenizer, TFBertForMaskedLM
from keras.callbacks import ModelCheckpoint
from transformers import *
import keras
from TFRecordRead_Class import TFRecordRead
from mySecrets import secrets
    

print("Num GPUs Available: ", len(tf.config.list_physical_devices('GPU')))
print(tf.config.list_physical_devices('GPU'))
AUTOTUNE = tf.data.experimental.AUTOTUNE
BATCH_SIZE = 128
MAX_SEQ_LEN = 100
VECTOR_SIZE = 512
USER_STATS_SIZE = 6

# load runtime arguments
USE_VECTOR = sys.argv[1] # Boolean, whether to use user vector inputs
USE_STATS = sys.argv[2] # Boolean, whether to use user stats inputs
modelType = sys.argv[3] # string, either 'age','place', or 'health'

if(modelType=='age'):
    PARENT_FOLDER = secrets['CHILD_FOLDER']
elif(modelType=='place'):
    PARENT_FOLDER = secrets['PLACE_FOLDER']
else:
    PARENT_FOLDER = secrets['HEALTH_FOLDER']
def loadDataset(dataFolder,vector=False,userStats=False):
    recordReader = TFRecordRead(dataFolder,BATCH_SIZE,vector,userStats)
    dataset = recordReader.tfRecords.prefetch(buffer_size=AUTOTUNE)
    dataset = dataset.batch(BATCH_SIZE,deterministic=False)
    print("loaded dataset")
    return(dataset)


def build_base():
    t_i = tf.keras.layers.Input((MAX_SEQ_LEN), dtype=tf.int32, name='input_ids')
    m_i = tf.keras.layers.Input((MAX_SEQ_LEN,), dtype=tf.int32, name='atttention_mask')
    model = TFBertForMaskedLM.from_pretrained(secrets['EXPANDED_BERT_BASE_FILEPATH'])(input_ids=t_i,attention_mask=m_i)
    model = tf.keras.Model([t_i,m_i],model)
    return model 

def saveBERTWeights(model):
    new_model = model.layers[1:][1]
    new_model.save_pretrained(secrets['PRETRAINED_BERT_BASE_FILEPATH'])

def loadBertWeights(baseModel):
    saveBERTWeights(baseModel)

def buildClassifictionModel(vector=False,userStats=False,fixWeights=False,nOutputs=6):
    MAX_SEQ_LEN = 100
    #baseModel = build_base()
    #baseModel.load_weights(weightFile)
    t_i = tf.keras.layers.Input((MAX_SEQ_LEN), dtype=tf.int32, name='input_ids')
    m_i = tf.keras.layers.Input((MAX_SEQ_LEN,), dtype=tf.int32, name='atttention_mask')
    inVals = [t_i,m_i]
    if(vector):
        v_i = tf.keras.layers.Input((VECTOR_SIZE), dtype=tf.float32, name='vector')
        inVals.append(v_i)
    if(userStats):
        u_i = tf.keras.layers.Input((USER_STATS_SIZE), dtype=tf.float32, name='user_stats')
        inVals.append(u_i)
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
    if(vector):
        combinedOutput = tf.concat([combinedOutput,v_i],-1)
    if(userStats):
        combinedOutput = tf.concat([combinedOutput,u_i],-1)
    combinedOutput = tf.keras.layers.Dropout(0.2)(combinedOutput)
    newLayers = tf.keras.layers.Dense(128,activation='relu',name='activate1')(combinedOutput)
    newLayers = tf.keras.layers.Dropout(0.2)(newLayers)
    newLayers = tf.keras.layers.Dense(128,activation='relu',name='activate2')(newLayers)
    newLayers = tf.keras.layers.Dropout(0.2)(newLayers)
    final = tf.keras.layers.Dense(nOutputs,activation=None,name='categorical')(newLayers)
    bm2 = tf.keras.Model(inputs = [inVals],outputs = final)
    return(bm2)


def runModel(model):
    model.compile(
        loss=[tf.keras.losses.BinaryCrossentropy(from_logits=True)],
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001)
    )
    
    checkpoint = ModelCheckpoint(filepath=PARENT_FOLDER + secrets['MODEL_CHECKPOINT_FILEPATH'],
                                monitor='val_loss',
                                save_best_only=True,
                                save_weights_only=False,
                                verbose=1,
                                initial_value_threshold=  None
    )
                                
    model.fit(trainData,
                            batch_size=BATCH_SIZE,
                            validation_data=testData,
                            epochs=1000,
                            callbacks=[checkpoint],
                            shuffle=True    # shuffle=False to reduce randomness and increase reproducibility
    )


trainData = loadDataset(PARENT_FOLDER + secrets['TRAIN_DATA_SUBFOLDER'],USE_VECTOR,USE_STATS)
testData = loadDataset(PARENT_FOLDER + secrets['TEST_DATA_SUBFOLDER'],USE_VECTOR,USE_STATS)

with tf.device('/GPU:0'):
    classificationModel = buildClassifictionModel(USE_VECTOR,USE_STATS)
    
    print(classificationModel.summary())

    runModel(classificationModel)