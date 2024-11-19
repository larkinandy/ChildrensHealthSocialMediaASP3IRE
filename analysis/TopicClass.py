### TopicClass.py
### Author: Andrew Larkin
### Date Created: Nov 13th, 2024
### Summary: Class for identifying social media post topics


# import libraries
from top2vec import Top2Vec
import pandas as ps
import numpy as np
try:
    import torch
except Exception as e:
    print("warning: pytorch not found. the Topic class functionality may not run properly")

class Topic:

    # create an instance of the Topic class
    # INPUTS:
    #    queryFolder (str) - absolute filepath where topic data is stored
    #    modelType (str) - unique id for a previously saved Top2Vec model
    #    loadWordKeys (boolean) - whether to load key lists for previously derived topics
    #    debug (boolean) - whether to load subsets of data and print output statements
    def __init__(self, queryFolder,modelType = None,loadWordKeys=False,debug=False):
        self.queryFolder=queryFolder
        self.debug=debug
        self.model = None

        # load word list for each topic 
        if(loadWordKeys):
            self.wordKeys = self.queryFolder + "topics_words.npy"
           

        # load topic model
        if(modelType!=None):
            self.model = Top2Vec.load(self.queryFolder + "topic" + modelType + "Model")

        self.umap_args = {
            'n_neighbors': 10,
            'n_components': 5,
            'metric': 'cosine'
        }

        self.hdbscan_args = {
            'min_cluster_size': 10,
            'min_samples':5,
            'metric': 'euclidean',
            'cluster_selection_method': 'eom'
        }

        if self.debug():
            print("top2vec computational device: %s " %(torch.cuda.get_device_name(0)))
    
    # create a topic model from a set of social media posts
    # INPUTS:
    #    nRecords (int) - number of records to sample for creating the topic model. Anecdotally, 5 million records was the maximum
    #                     for the author's workstation with 256GB ram.
    def createTopicModel(self,nRecords):

        # social media posts were previously exported from neo4j and saved as csv
        posts = ps.read_csv(self.queryFolder + "/tweet_hit_text.csv",usecols='t.orig_text',nrows=nRecords)

        # remove posts with no text (pictures aren't included in the topic model)
        posts.dropna(inplace=True)

        # create the topic model
        model = Top2Vec(
            documents= posts, 
            min_count = 1000, # only consider words with 1000+ uses among the posts dataset
            workers=14, # 14 cpu threads
            embedding_model='distiluse-base-multilingual-cased',  # BERT language model
            umap_args = self.umap_args, 
            hdbscan_args = self.hdbscan_args
        )

        # save the full model for debug purposes. Too big for practical use
        model.save(self.queryFolder + "fullModel" + str(nRecords))

        # reduce model to 5000 topics and save
        model.hierarchical_topic_reduction(5000)
        model.save(self.queryFolder + "reducedModel5000")
        self.model=model

        # create a key-value dict for assigning a unique id to each topic. 
        # needed because top2vec dynamically changes topic ids based on rank 
        topics_words,word_scores,topic_nums = model.get_topics(reduced=True)
        self.wordKeys = topics_words

        # save key-value dict
        np.save(self.queryFolder + "/topics_words.npy", topics_words)

    # given a word list for a social media post, find the topic index. Necessary since topic id numbers 
    # in the top2vec model change every time the topic rank order changes
    # INPUTS:
    #    words (str array) - 50 words most closely associated with the topic, sorted by simiarlity score
    # OUTPUTS:
    #    topic index that is static and does not change. Defined by the word key stored on disk
    def mapFindTopicIndex(self,words):
        return np.flatnonzero(np.all(self.word_keys == words, axis=1))[0]
    
    # given a subset batch of social media posts, identify the primary topic for each post and return the topic index and similarity score
    # INPUTS:
    #    posts (pandas dataframe) - set of social media posts. DF includes unique id and text
    # OUTPUTS:
    #    postTopics (pandas dataframe) - topic index, similarity score, and unique id for each post
    def getPostTopicsBatch(self,posts):
        nPosts = posts.count().iloc[0]
        if(self.model==None):
            print("cannot get topics for posts: no topic model has been loaded into memory")
            return(None)
        
        # remove old documents except for 1. 1 doc is needed for top2vec models to retain the variable .document_ids
        docIds = self.model.document_ids
        self.model.delete_documents(docIds[1:])

        # add new documents of interest
        self.model.add_documents(list(posts['t.orig_text']))

        # get topic words for docs of interest
        topic_nums, topic_score, wordArr,_ = self.model.get_documents_topics(list(range(1,nPosts+1)),num_topics=1,reduced=True)

        # using topic words, get topic ids for docs of interest and save to csv
        topic_nums = list(map(self.mapFindTopicIndex,wordArr))
        postTopics = ps.DataFrame({
            't.id':posts['t.id'].astype(str),
            'topic':topic_nums,
            'score':topic_score
        })
        return(postTopics)

    # given all social media posts, identify the primary topic for each post and save to csv
    # INPUTS:
    #    chunksize (int) - number of records to process at a time. Limited by workstation RAM
    #    inputFile (str) - absolute filepath where social media posts are stored in .csv format
    #    outputFile (str) - absolute filepath where post topics will be stored in .csv format
    def getPostTopics(self,chunksize,inputFile,outputFile):
        topicArr = []
        index=0

        # for each batch of data, get the topic indexes and add to topic array
        for chunk in ps.read_csv(inputFile, chunksize=chunksize,usecols=['t.orig_text','t.id'],dtype={
            't.orig_text': 'string',
            't.id': 'string'
        }):
            chunk.dropna(inplace=True)
            topicArr.append(self.getPostTopicsBatch(chunk))
            index+=1

            # if debugging, save intermediate records every 10 batches
            if self.debug:
                print(index)
                if(index%10==0):
                    df = ps.concat(topicArr)
                    df.to_csv(outputFile,index=False)

        # convert topic array to pandas dataframe and then save to csv
        df = ps.concat(topicArr)
        df.to_csv(outputFile,index=False)

# end of TopicClass.py