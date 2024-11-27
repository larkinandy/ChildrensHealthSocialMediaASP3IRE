import pandas as ps
import os
import sys
import numpy as np 
from transformers import BertTokenizer, TFBertForMaskedLM
import tensorflow as tf
import transformers
import glob
from mySecrets import secrets

MODEL_FILEPATH = sys.argv[1] # Absolute filepath to dl model
CATEGORY = sys.argv[2] # type of model: place, age, or health

def mapTextExtract(tweet):
    tmpText = tweet['text'].replace("\n",". ")
    return(tmpText + "\n")

def mapConvExtract(tweet):
    try:
        convid = tweet['conversation_id']
    except:
        convid = 'nope'
    return(convid)

def mapIdExtract(tweet):
    return(tweet['id'])

def loadSingleBatch(filename):
    tweetBatch = np.load(filename,allow_pickle=True)
    text,convId,tweetId = [],[],[]
    for subBatch in tweetBatch:
        text +=  list(map(mapTextExtract,subBatch)
        convId += list(map(mapConvExtract,subBatch))
        tweetId += list(map(mapIdExtract,subBatch))
    df = ps.DataFrame({
        'text':text,
        'convId':convId,
        'tweetId':tweetId
    })
    if(df.count()[0]>0):
        df = df[~df['convId'].str.contains('nope')]
    return(df)

def setupModelInputs(tweetData):
    tweetText = list(tweetData['text'])
    inputs = TOKEN(tweetText,max_length=100,truncation=True,padding='max_length',return_tensors="tf")
    inp_ids = tf.convert_to_tensor(inputs['input_ids'])
    inputs['input_ids'] = inp_ids
    return(inputs)

def combineSingleDay(inFile):
    text = []
    npData = np.load(inFile,allow_pickle=True)
    for batch in npData:
        text += list(map(mapTextExtract,batch))
    print("found %i tweets for day %s" %(len(text),inFile))
    return(text)

def processSingleYearSingleVariable(year,variable,model):
    inFolder = secrets['TWEET_STORE'] + str(year) + "/" + str(variable) + "/"
    outFolder = secrets['PRED_STORE'] + str(year) + "/" + str(variable) + "/"
    tweetFiles = glob.glob(inFolder + "tw_*")
    for file in tweetFiles:
        dateStamp = file[file.rfind('_')+1:-4]
        outputFile = outFolder + "re_" + dateStamp + ".csv"
        if(os.path.exists(outputFile)):
            print("%s already exists" %(outputFile))
        else:
            processSingleBatch(file,outputFile,model)

modelFile = MODEL_FILEPATH
relaxedBERT = tf.keras.models.load_model(modelFile,custom_objects={"TFBertModel": transformers.TFBertModel})
relaxedBERT.compile(loss=[tf.keras.losses.BinaryCrossentropy()],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])
TOKEN = BertTokenizer.from_pretrained(secrets['BERT_TOKENBASE'])

def processSingleConvFolder(folder,model):
    inFolder = secrets['CONVERSATION_FOLDER'] + folder + "/"
    outFolder = secrets['CONVERSATION_PRED_FOLDER'] + folder + "/"
    tweetFiles = glob.glob(inFolder + "tw_*")
    for file in tweetFiles:
        convId = file[file.rfind('_')+1:-4]
        outputFile = outFolder + "re_" + convId + ".csv"
        if(os.path.exists(outputFile)): 
            return
        else:
            if(CATEGORY=='place':)
                processSinglePlaceBatch(file,outputFile,model)
            elif(CATEGORY=='health'):
                processSingleHealthBatch(file,outputFile,model)
            elif(CATEGORY=='age'):
                processSingleBatchAge(file,outputFile,model)
            else:
                print("%s is not a valid category" %(CATEGORY))

def processSingleBatchPlace(infFile,outFile,model):
    df = loadSingleBatch(inFile)
    if(df.count()[0]==0):
        return
    modelInputs = setupModelInputs(df)
    preds = model.predict([modelInputs['input_ids'],modelInputs['attention_mask']])
    df['isDaycare'] = tf.nn.sigmoid(preds[:,0])
    df['isPark'] = tf.nn.sigmoid(preds[:,1])
    df['isHome'] = tf.nn.sigmoid(preds[:,2])
    df['isSchool'] = tf.nn.sigmoid(preds[:,3])
    df['isNeighborhood'] = tf.nn.sigmoid(preds[:,4])
    df['isIndoor'] = tf.nn.sigmoid(preds[:,5])
    df['isOutdoor'] = tf.nn.sigmoid(preds[:,6])
    df2 = df.drop(['text'],axis=1)
    df2.to_csv(outFile,index=False)

def processSingleBatchHealth(inFile,outFile,model):  
    df = loadSingleBatch(inFile)
    if(df.count()[0]==0):
        return
    modelInputs = setupModelInputs(df)
    preds = model.predict([modelInputs['input_ids'],modelInputs['attention_mask']])
    df['isCognitive'] = tf.nn.sigmoid(preds[:,0])
    df['isEmotional'] = tf.nn.sigmoid(preds[:,1])
    df['isPhysical'] = tf.nn.sigmoid(preds[:,2])
    df['isPositive'] = tf.nn.sigmoid(preds[:,3])
    df['isNegative'] = tf.nn.sigmoid(preds[:,4])
    df2 = df.drop(['text'],axis=1)
    df2.to_csv(outFile,index=False)

def processSingleBatchAge(inFile,outFile,model):  
    df = loadSingleBatch(inFile)
    if(df.count()[0]==0):
        return
    modelInputs = setupModelInputs(df)
    preds = model.predict([modelInputs['input_ids'],modelInputs['attention_mask']])
    df['isChild'] = tf.nn.sigmoid(preds[:,0])
    df['isBaby'] = tf.nn.sigmoid(preds[:,1])
    df['isToddler'] = tf.nn.sigmoid(preds[:,2])
    df['isElem'] = tf.nn.sigmoid(preds[:,3])
    df['isMiddle'] = tf.nn.sigmoid(preds[:,4])
    df['isHigh'] = tf.nn.sigmoid(preds[:,5])
    df2 = df.drop(['text'],axis=1)
    df2.to_csv(outFile,index=False)

parentConvFolder = secrets['CONVERSATION_FOLDER']
outputFolder = secrets['CONVERSATION_PRED_FOLDER']
conversationFolders = os.listdir(parentConvFolder)
index = 0
for folder in conversationFolders:
    curFolder = parentConvFolder + folder + "/"
    if not(os.path.exists(outputFolder + folder)):
        os.mkdir(outputFolder + folder)
    processSingleConvFolder(folder,relaxedBERT)
    index+=1
    if(index%100==0):
        print(index)

for year in reversed(range(2014,2022)):
    yearFolder = secrets['PRED_STORE'] + str(year) + "/"
    if not(os.path.exists(yearFolder)):
        os.mkdir(yearFolder)
    for cat in['age','place','env','health','health2']:
        print("calculating values for year %i and category %s" %(year,cat))
        catFolder = yearFolder + "/" + str(cat)
        if not(os.path.exists(catFolder)):
            os.mkdir(catFolder)
        processSingleYearSingleVariable(year,cat,relaxedBERT)