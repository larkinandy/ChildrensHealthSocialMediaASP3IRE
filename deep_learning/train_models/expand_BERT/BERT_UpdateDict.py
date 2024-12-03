# BERT_UpdateDict.py
# Author: Andrew Larkin

# Summary: add 100 most popular emojis in social media dataset
#          to BERT model and tokenizer

from transformers import *
import tensorflow as tf
import os
import tensorflow as tf
import pandas as ps

# import secrets
from mySecrets import secrets 


# save expanded BERT model tokenizer and model
# INPUTS:
#    saveModel (tf model) - BERT model to save
#    inToken (tokernizer) - BERT tokenizer to save
def saveBaseModel(saveModel,inToken):
    inToken.save_pretrained(f"{'expandedTokenBase'}")
    saveModel.save_pretrained(f"{'expandedModelBase'}")

# add emojis to BERT model and tokenizer
# INPUTS:
#    newTokens (str list) - list of emjois to add to tokenizer and model
def updateTokenizer(newTokens):
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    model = TFBertForMaskedLM.from_pretrained('bert-base-uncased')
    added_tokens = tokenizer.add_tokens(newTokens)
    print("tokenizer vocab size: %i \n added tokens: %i" %(len(tokenizer),added_tokens))
    
    # resize the embeddings matrix of the model 
    model.resize_token_embeddings(len(tokenizer))
    saveBaseModel(model,tokenizer)

# main function
if __name__ == "__main__":
    newTokens = list(ps.read_csv(secrets['EMOJI_LIST'])['emojis'])
    updateTokenizer(newTokens)


# end of BERT_UpdateDict.py