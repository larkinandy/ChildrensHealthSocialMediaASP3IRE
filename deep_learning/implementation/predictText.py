#predictText.py 
# Author: Andrew Larkin

# Summary: predict age,place,and health from social media text

# import libraries
import pandas as ps
import os
import sys
import numpy as np 
from transformers import BertTokenizer, TFBertForMaskedLM
import tensorflow as tf
import transformers
import glob

# import secrets
from mySecrets import secrets

# runtime arguments, which differ depending on the deep learning model
MODEL_FILEPATH = sys.argv[1] # Absolute filepath to dl model
CATEGORY = sys.argv[2] # type of model: place, age, or health

# replace \n in social media text with '. '
# INPUTS:
#    post (json dict) - single social media post
# OUTPUTS:
#    text with new line characters replaced with '. '
def mapTextExtract(post):
    tmpText = post['text'].replace("\n",". ")
    return(tmpText + "\n")

# get conversation id from json record
# INPUTS:
#    post (json dict) - single social media post
# OUTPUTS:
#    convid (str) - conv id, stored in str format
def mapConvExtract(post):
    try:
        convid = post['conversation_id']
    except:
        convid = 'nope'
    return(convid)

# get social media post id from json record
# INPUTS:
#    post (json dict) - single social media post
# OUTPUTS:
#    id (str) - social media post id
def mapIdExtract(post):
    return(post['id'])

# load single batch of social media posts (all stored in a single npy file)
# INPUTS:
#    filename (str) - absolute filepath to json file containing posts
# OUTPUTS:
#    df (pandas dataframe) - contains posts reformated to pandas dataframe
def loadSingleBatch(filename):
    tweetBatch = np.load(filename,allow_pickle=True)
    text,convId,tweetId = [],[],[]
    for subBatch in tweetBatch:
        text +=  list(map(mapTextExtract,subBatch))
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

# tokenize text and setup attention masks 
# INPUTS:
#    postData (json dict) - contains social media posts and metadata in json format
# OUTPUTS:
#    BERT tokens and attention masks created from social media text
def setupModelInputs(postData):
    tweetText = list(postData['text'])
    inputs = TOKEN(tweetText,max_length=100,truncation=True,padding='max_length',return_tensors="tf")
    inp_ids = tf.convert_to_tensor(inputs['input_ids'])
    inputs['input_ids'] = inp_ids
    return(inputs)

# for a single day of social media posts in json format, extract text from posts and store as a list
# INPUTS:
#    inFile (str) - absolute filepath to file containing 1 day of social media posts
# OUTPUTS:
#    str list of social media text, one element for each post
def combineSingleDay(inFile):
    text = []
    npData = np.load(inFile,allow_pickle=True)
    for batch in npData:
        text += list(map(mapTextExtract,batch))
    print("found %i tweets for day %s" %(len(text),inFile))
    return(text)

# predict place labels for a single batch of social media posts
# INPUTS:
#    inFile (str) - absolute filepath to posts, stored in npy format
#    outFile (str) - absolute filepath where model predictions will be stored
#    model (custom object) - tensorflow model used to predict place labels from text
def processSingleBatchPlace(inFile,outFile,model):
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

# predict health labels for a single batch of social media posts
# INPUTS:
#    inFile (str) - absolute filepath to posts, stored in npy format
#    outFile (str) - absolute filepath where model predictions will be stored
#    model (custom object) - tensorflow model used to predict health labels from text
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

# predict age labels for a single batch of social media posts
# INPUTS:
#    inFile (str) - absolute filepath to posts, stored in npy format
#    outFile (str) - absolute filepath where model predictions will be stored
#    model (custom object) - tensorflow model used to predict age labels from text
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

# for a single category of tweet downloads and a single year of data, predict 
# labels for a single deep learning model (age, place, or health)
# INPUTS:
#    year (int) - calendar year to process
#    variable (str) - category of tweet downloads to process
#    model (custom object) - tensorflow model to predict labels from text
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
            if(CATEGORY=='place'):
                processSingleBatchPlace(file,outputFile,model)
            elif(CATEGORY=='health'):
                processSingleBatchHealth(file,outputFile,model)
            elif(CATEGORY=='age'):
                processSingleBatchAge(file,outputFile,model)
            else:
                print("%s is not a valid category" %(CATEGORY))

# process tweet downloads for a single conversation
# INPUTS:
#    folder (str) - folderpath where posts for the conversation are stored
#    model (custom objecdT) - tensorflow model to predict labels from text
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
            if(CATEGORY=='place'):
                processSingleBatchPlace(file,outputFile,model)
            elif(CATEGORY=='health'):
                processSingleBatchHealth(file,outputFile,model)
            elif(CATEGORY=='age'):
                processSingleBatchAge(file,outputFile,model)
            else:
                print("%s is not a valid category" %(CATEGORY))

# main function
if __name__ == '__main__':

    # load model and compile
    modelFile = MODEL_FILEPATH
    relaxedBERT = tf.keras.models.load_model(modelFile,custom_objects={"TFBertModel": transformers.TFBertModel})
    relaxedBERT.compile(loss=[tf.keras.losses.BinaryCrossentropy()],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])
    TOKEN = BertTokenizer.from_pretrained(secrets['BERT_TOKENBASE'])

    parentConvFolder = secrets['CONVERSATION_FOLDER']
    outputFolder = secrets['CONVERSATION_PRED_FOLDER']
    conversationFolders = os.listdir(parentConvFolder)
    index = 0

    # predict labels for tweets that were downloaded by conversation id
    for folder in conversationFolders:
        curFolder = parentConvFolder + folder + "/"
        if not(os.path.exists(outputFolder + folder)):
            os.mkdir(outputFolder + folder)
        processSingleConvFolder(folder,relaxedBERT)
        index+=1
        if(index%100==0):
            print(index)

    # predict labels for tweets that were downloaded by keyword 
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

# end of predictText.py