# AnalysisClass.py 
# Author: Andrew Larkin
# Summary: Perform analyses on a Neo4j database containing tweets pertinent
# to children's health.  Part of a set of classes for the ASP3IRE project


from TweetImageClass import TweetImage
from GraphDBClass import GraphDAO
from NetworkClass import Network
from TopicClass import Topic
import pandas as ps


class Analyzer:

    # initialize instance of class
    # INPUTS: 
    #     storageFolder (str) - locations for flat file storage (images, metadata, etc)
    #     db_dict (dict) - credentials for connecting to neo4j db
    def __init__(self, storageFolder,networkFolder,topicFolder,dbDict):
        
        # setup paths for storing intermediary files.  Needed for building database, not 
        # needed during database analysis
        self.analysisFolder = storageFolder + "Analyses/"
        self.tweetImgProcesser = TweetImage(storageFolder + "ImageStore/")
        self.kwDict = self.loadKeywords(storageFolder + "keyword_csvs/")
        self.networkProcessor = Network()
        self.topicModeler = Topic(topicFolder,modelType ='5000',loadWordKeys=True,debug=False)

        # setup connection to Neo4j database.  This is needed for both database development
        # and analysis
        self.graphDBAPI = GraphDAO(dbDict['dbUri'],dbDict['dbUser'],dbDict['dbPW'])

    # load keywords used to search historical Twitter records 
    # INPUTS:
    #    keywordFolder (str) - absolute filepath where csv files containing keywords are stored
    # OUTPUTS:
    #    kwDict (dict) - contains keywords for multiple categories, one key for each categories
    def loadKeywords(self,keywordFolder):
        ageKeywords = list(ps.read_csv(keywordFolder + "age.csv")['keywords'])
        placeKeywords = list(ps.read_csv(keywordFolder + "place.csv")['keywords'])
        healthKeywords = list(ps.read_csv(keywordFolder + "health.csv")['keywords'])
        healthKeywords2 = list(ps.read_csv(keywordFolder + "health2.csv")['keywords'])
        kwDict = {
            'age':ageKeywords,
            'place':placeKeywords,
            'health':healthKeywords,
            'health2':healthKeywords2
        }
        return(kwDict)

    # get number of times a word appears in the text of tweets stored in the datbase
    # INPUTS:
    #    kw (str) - word to search for.  Can only be one string, not combinations
    # OUTPUTS:
    #    count (int) - number of times word appears in tweet text
    def getCountKeywordIncidence(self,kw):
        count = self.graphDBAPI.countKeywordIncidence(kw)
        return(count)

    # iterate through all keywords in a category, counting number of times keywords appear in tweet text
    # INPUTS:
    #    cat (str) - caategory of interest
    #    saveToDisk (boolean) - True is results should be saved to disk, False otherwise
    def countEssentialKeywordIncidence(self,cat,saveToDisk=True):
        if(cat not in self.kwDict.keys()):
            print("%s is not a keyword category.  Options are: %s" %(cat,self.kwDict.keys()))
            return
        print("calculating keyword counts for category %s" %(cat))
        keywords = self.kwDict[cat]
        counts = []

        # TODO: this should be parallelizable.  Look into parallelizing for speedup
        for kw in keywords:
            counts.append(self.getCountKeywordIncidence(kw))
        df = ps.DataFrame({
            'keyword':keywords,
            'count':counts
        }) 

        # save keyword counts to disk
        if(saveToDisk):
            outputFilename = self.analysisFolder + cat + "_count.csv"
            df.to_csv(outputFilename,index=False)

        print("completed calculating keyword counts")
        return(df)

    # get a random sample of tweets containing a keyword
    # INPUTS:
    #    kw (str) - keyword tweets should contain
    #    sampSize (int) - number of tweets to randomly sample
    # OUTPUTS:
    #    list containing random sample of tweets
    def getRandomSampleKeyword(self,kw,sampSize):
        return(self.graphDBAPI.getKeywordRandomSample(kw,sampSize))

    # reformat a list of randomly sampled tweets
    # INPUTS:
    #    randSamp (array) - list of randomly sampled tweets
    #    imgFilenames (str array) - filename if images were attached to tweet
    #    sampKW (str) - keyword included in all random samples
    #    sampCat (str) - tweet keyword category
    # OUTPUTS:
    #    pandas dataframe containing reformated tweets
    def randomSampleReformat(self,randSamp,imgFilenames,sampKW,sampCat):
        imgName,text,id,year,kw,cat=[[] for x in range(6)]
        index = 0

        # for each randomly sampled tweet, reformat variables
        for record in randSamp:
            if(imgFilenames[index]!='None'):
                imgName.append(imgFilenames[index])
                text.append(record['text'])
                id.append(record['id'])
                year.append(record['created_at'][0:4])
                kw.append(sampKW[index])
                cat.append(sampCat[index])
            else:
                print("not include tweet id %s in sample due to missing media key" %(record['id']))
            index+=1

        # create dataframe using reformatted data    
        df = ps.DataFrame({
            'id':id,
            'text':text,
            'img_name':imgName,
            'year':year,
            'kw':kw,
            'cat':cat
        })
        return(df)

    # for a category of interest, get a random selection of tweets for all keywords in the category
    # INPUTS:
    #    cat (str) - category of interest
    #    sampSize (int) number of tweets to randomly sample for the entire category
    # OUTPUTS:
    #    tweetSample (array) - list of randomly sampled tweets
    #    tweetKW (str array) - the kewyord for each tweet in tweetSample
    #    tweetCat (str array) - the category for each tweet in tweetSample
    def getRandomSampleCategory(self,cat,sampSize):

        # should only proceed with categories defined by the keyword dictionary
        if(cat not in self.kwDict.keys()):
            print("%s is not a keyword category.  Options are: %s" %(cat,self.kwDict.keys()))
            return

        keywords = self.kwDict[cat]
        sizePerKW = int(sampSize/len(keywords))
        tweetSample,tweetKW = [],[]
        print("creating random sample for %s keywords" %(cat))
        print("sampling %i tweets per keyword" %(sizePerKW))
        
        # get a  random sample for each keyword in the category
        for curKW in keywords:
            tempSample = self.getRandomSampleKeyword(curKW,sizePerKW)
            tweetSample += tempSample
            tweetKW+= [curKW for x in range(len(tempSample))]
        print("sampled %i records for keyword category %s" %(len(tweetSample),cat))
        tweetCat = [cat for x in range(len(tweetSample))]
        return([tweetSample,tweetKW,tweetCat])

    # copy images from the main data store to a folder for training data
    # INPUTS:
    #    trainingDict (dictionary) - dictionary containing training tweets, including image filenames
    # OUTPUTS: 
    #    training image filenames, or "SN.png" if the tweet doesn't have an image
    def copySampledImages(self,trainingDict):
        return(self.tweetImgProcesser.copyTrainingImages(trainingDict))

    # sample tweets for training, including setting up a training dataset folder
    # INPUTS:
    #    cat (str) - category to randomly sample tweets for
    #    sampSize (int) - number of tweets to randomly sample
    def sampleTweetsForTraining(self,cat,sampSize):

        # randomly sample tweets
        randomSamp,sampKW,sampCat = self.getRandomSampleCategory(cat,sampSize)
        
        # copy training images from main folder to training folder
        imgFilenames = self.copySampledImages(randomSamp)
        
        # reformat training data and save to csv
        sampDF = self.randomSampleReformat(randomSamp,imgFilenames,sampKW,sampCat)
        sampDF.to_csv(self.analysisFolder + "trainingSample_" + cat + ".csv",index=False)


# end of AnalysisClass.py