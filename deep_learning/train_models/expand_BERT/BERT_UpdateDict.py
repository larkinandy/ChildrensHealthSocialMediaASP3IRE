#from transformers import BertTokenizer, TFBertForMaskedLM
from transformers import *
import tensorflow as tf
import os
import numpy as np
import re
import matplotlib.pyplot as plt
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer# initialize our tokenizer with the Portuguese spaCY one
from spellchecker import SpellChecker
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text as text
import glob

def spacy_tokenizer(document):
    # tokenize the document with spaCY
    doc = nlp(document)
    # Remove stop words and punctuation symbols
    tokens = [
        token.text for token in doc if (
        token.is_stop == False and \
        token.is_punct == False and \
        token.text.strip() != '' and \
        token.text.find("\n") == -1)]
    return tokens

def clean_text(line):
    line = re.sub(r'-+',' ',line)
    line = re.sub(r'[^a-zA-Z, ]+'," ",line)
    line = re.sub(r'[ ]+'," ",line)
    line += "."
    return line

def screenNewWords(tokenizer,newWords):
    screenedList = list( filter((lambda x: check(x)==False and x not in tokenizer.vocab), newWords))
    return screenedList

def check(word):
    if word == spell.correction(word):
        return True
    else:
        return False
    
import pandas as ps
newTokens = list(ps.read_csv("/mnt/h/Aspire/BERT/emojis/emojiList.csv")['emojis'])
newTokens

def saveBaseModel(saveModel,inToken):
    inToken.save_pretrained(f"{'expandedTokenBase'}")
    saveModel.save_pretrained(f"{'expandedModelBase'}")

def updateTokenizer2():
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    model = TFBertForMaskedLM.from_pretrained('bert-base-uncased')
    #tokenizer = BertTokenizer.from_pretrained('testToken1')
    #model = TFBertForMaskedLM.from_pretrained('testModel1')
    newTokens = list(ps.read_csv("/mnt/h/Aspire/BERT/emojis/emojiList.csv")['emojis'])
    added_tokens = tokenizer.add_tokens(newTokens)
    print("tokenizer vocab size: %i \n added tokens: %i" %(len(tokenizer),added_tokens))
    
    # resize the embeddings matrix of the model 
    model.resize_token_embeddings(len(tokenizer))
    saveBaseModel(model,tokenizer)
updateTokenizer2()

def identifyNewTokens(inFile):
    # apply spaCY tokenizer through scikit-learn
    # https://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting
    tfidf_vectorizer = TfidfVectorizer(
        lowercase=False, 
        tokenizer=spacy_tokenizer, 
        norm='l2', 
        use_idf=True, 
        smooth_idf=True, 
        sublinear_tf=False
    )
    
    # parse matrix of tfidf
    length = len(inFile)
    result = tfidf_vectorizer.fit_transform(inFile)

    # get idf of tokens
    idf = tfidf_vectorizer.idf_

    # get tokens from most frequent in documents to least frequent
    idf_sorted_indexes = sorted(range(len(idf)), key=lambda k: idf[k])
    idf_sorted = idf[idf_sorted_indexes]
    
    tokens_by_df = np.array(tfidf_vectorizer.get_feature_names_out())[idf_sorted_indexes]
    # choose the proportion of new tokens to add in vocabulary
    pct = 1 # all tokens present in at least 1%


    index_max = len(np.array(idf)[np.array(idf)>=pct])
    newTokens = tokens_by_df[:index_max]
    newTokens = list(set(newTokens))
    return(newTokens)

spell = SpellChecker()
# define spaCY tokenizer
nlp = spacy.load(
    "en_core_web_lg", 
    exclude=['morphologizer', 'parser', 'ner', 'attribute_ruler', 'lemmatizer']
    )
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')

folder = '/mnt/h/Aspire/BERT/BERT_text/'
tweetFiles = glob.glob(folder + "txt_*")
print(tweetFiles)
with open(tweetFiles[0],'r') as f:
    file = f.read().split("\n")
print(file[0:5])

newTokens = identifyNewTokens(file)
newTokens = screenNewWords(tokenizer,newTokens)
newTokens = newTokens[:100]

def mapInputIds(inp):
    actual_tokens = list(set(range(100)) - set(np.where((inp == 101) | (inp == 102) | (inp == 0))[0].tolist()))
    #We need to select 15% random tokens from the given list
    num_of_token_to_mask = int(len(actual_tokens)*0.15)
    token_to_mask = np.random.choice(np.array(actual_tokens), size=num_of_token_to_mask, replace=False).tolist()
    #Now we have the indices where we need to mask the tokens
    inp[token_to_mask] = 103
    return(inp)

def createCustomTokenizer(newWordList,debug=False):
    # import a model and tokenizer
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    model = TFBertForMaskedLM.from_pretrained('bert-base-uncased')
    #tokenizer = BertTokenizer.from_pretrained('testToken1')
    #model = TFBertForMaskedLM.from_pretrained('testModel1')
    
    newTokens = screenNewWords(tokenizer,newWordList)
    newTokens = newTokens[:100]

    ## add new tokens to the existing vocabulary (only those not already presents)
    added_tokens = tokenizer.add_tokens(newTokens)
    if(debug):
        print("tokenizer vocab size: %i \n added tokens: %i" %(len(tokenizer),added_tokens))
    
    # resize the embeddings matrix of the model 
    model.resize_token_embeddings(len(tokenizer))
    return(model,tokenizer,newTokens)

def setupModelInputs(tokenizer,inFile,debug=False):
    inputs = tokenizer(file,max_length=100,truncation=True,padding='max_length',return_tensors='tf')
    inputs['labels'] = inputs['input_ids']
    if debug: 
        print(inputs.keys())
    inp_ids = list(map(mapInputIds,inputs.input_ids.numpy()))
    inp_ids = tf.convert_to_tensor(inp_ids)
    inputs['input_ids'] = inp_ids

    return(inputs)



newTokens = identifyNewTokens(file)

newTokens = screenNewWords(tokenizer,newTokens)
newTokens = newTokens[:100]
model,tokenizer,newTokens = createCustomTokenizer(newTokens,debug=True)
inputs = setupModelInputs(tokenizer,file,debug=True)

model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True))
history = model.fit([inputs.input_ids,inputs.attention_mask],inputs.labels,verbose=1,batch_size=32,epochs=2)
print(model)

def saveModel(saveModel,inToken,folder,iteration):
    inToken.save_pretrained(f"{'testToken' + str(iteration)}")
    saveModel.save_pretrained(f"{'testModel' + str(iteration)}")

saveModel(model,tokenizer,'a',1)

