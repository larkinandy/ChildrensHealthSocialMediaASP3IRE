### TweetIngestClass.py
### Author: Andrew Larkin
### Date Created: May 18, 2022
### Summary: Perform ops needed to ingest data into Neo4j database

from TwitterAPI import TwitterAPI
from TweetImageClass import TweetImage
from GraphDBClass import GraphDAO
import glob
import numpy as np
import pandas as ps
import json
import os

class TweetIngest:

    # initialize instance of class
    # INPUTS: 
    #     api_dict (dict) - key values needed for Twitter API access
    #     storageFolder (str) - locations for flat file storage (images, metadata, etc)
    #     db_dict (dict) - credentials for connecting to neo4j db
    def __init__(self, apiDict,storageFolder,dbDict,year):
        
        # setup paths for storing intermediary files.  Needed for building database, not 
        # needed during database analysis
        self.storageFolder = storageFolder
        self.imageFolder = storageFolder + "ImageStore/" + str(year) + "/"
        self.tweetFolder = storageFolder + "TweetStore/" + str(year) + "/"
        self.userTweetsFolder = storageFolder + "TweetStore/users/"
        self.conversationTweetFolder = storageFolder + "TweetStore/conversations/"
        self.placeFolder = storageFolder + "PlaceStore/"
        self.userFolder = storageFolder + "UserStore/"
        self.predictionsFolder = storageFolder + "ModelPredictions/PredStore/"
        self.keywordFolder = storageFolder + "keyword_csvs/"
        self.tweetImgProcesser = TweetImage(self.imageFolder)

        # setup interacting with the API
        self.API_KEY = apiDict['API_KEY']
        self.SECRET = apiDict['SECRET']
        self.twitterAPI = TwitterAPI(
            self.API_KEY,self.SECRET,self.imageFolder,self.tweetFolder,
            self.keywordFolder,self.placeFolder,self.userFolder,self.userTweetsFolder,self.conversationTweetFolder
        )

        # setup connection to Neo4j database.  This is needed for both database development
        # and analysis
        self.graphDBAPI = GraphDAO(dbDict['dbUri'],dbDict['dbUser'],dbDict['dbPW'])


    def downloadTweetsForUsers(self,userIds,year):
        self.twitterAPI.processTweetsForUsers(userIds,year)

    def downloadTweetsForConversations(self,convIds):
        self.twitterAPI.processTweetsForConversations(convIds)


    # given a folder filled with metadata about images, load metadata and download images to
    # a hashed folder location
    # INPUTS:
    #     metaFolder (str) - absolute filepath to folder containing metadata files
    def downloadImagesFromMetaFolder(self,metaFolder,processedFile=None):
        self.tweetImgProcesser.downloadImagesFromMetaFolder(metaFolder,processedFile)

    # given a folder filled with metadata about images, load metadata and download images to
    # a hashed folder location
    # INPUTS:
    #     metaFolder (str) - absolute filepath to folder containing metadata files
    def downloadConversationImages(self):
        folder = self.conversationTweetFolder
        processedListFolder = folder + "processed/"
        for subfolder in range(5000):
            curFolder = folder + str(subfolder) + "/"
            processFile = processedListFolder + "processed_images_" + str(subfolder) + ".csv"
            processedList = self.tweetImgProcesser.downloadImagesFromMetaFolder(curFolder,processedList)
            processedList.to_csv(processFile,index=False)


    # given a folder filled with metadata about images, load metadata and download images to
    # a hashed folder location
    # INPUTS:
    #     metaFolder (str) - absolute filepath to folder containing metadata files
    def downloadUserImages(self):
        folder = self.userTweetsFolder
        processedListFolder = folder + "processed/"
        for subfolder in range(5000):
            curFolder = folder + str(subfolder) + "/"
            processFile = processedListFolder + "processed_images_" + str(subfolder) + ".csv"
            if(os.path.exists(processFile)):
                processedList = list(ps.read_csv(processFile)['processedImages'])
            else:
                processedList = []
            processedList = self.tweetImgProcesser.downloadImagesFromMetaFolder(curFolder,processedList)
            processedList.to_csv(processFile,index=False)


    # given a folder where tweet metafiles are stored, load metafiles, process, and store in Neo4j
    def ingestConversationTweetsFromHashFolder(self):
        folder = self.conversationTweetFolder
        processedListFolder = folder + "processed/"
        for subfolder in range(5000):
            curFolder = folder + str(subfolder) + "/"
            metaFiles = glob.glob(curFolder + "tw_*")
            processFile = processedListFolder + "processed_list_" + str(subfolder) + ".csv"
            processedList = list(ps.read_csv(processFile)['processedFiles'])
            print("found %i twitter files for subfolder %i" %(len(metaFiles),subfolder))
            for metaFile in metaFiles:
                shortName = metaFile[metaFile.rfind("\\")+1:]
                if(shortName not in processedList):
                    print("processing tweets for twitter file %s" %(metaFile))
                    self.processTweetBatch(metaFile)
                    print("finished processing tweets for twitter file %s" %(metaFile))
                    processedList.append(metaFile[metaFile.rfind("\\")+1:])
                    processedDict = ps.DataFrame({'processedFiles':processedList})
                    processedDict.to_csv(processFile,index=False)

    # given a folder where tweet metafiles are stored, load metafiles, process, and store in Neo4j
    def ingestUserTweetsFromHashFolder(self):
        folder = self.userTweetsFolder
        processedListFolder = folder + "processed/"
        for subfolder in range(5000):
            curFolder = folder + str(subfolder) + "/"
            metaFiles = glob.glob(curFolder + "tw_*")
            processFile = processedListFolder + "processed_list_" + str(subfolder) + ".csv"
            if(os.path.exists(processFile)):
                processedList = list(ps.read_csv(processFile)['processedFiles'])
            else:
                processedList = []
            print("found %i twitter files for subfolder %i" %(len(metaFiles),subfolder))
            for metaFile in metaFiles:
                shortName = metaFile[metaFile.rfind("\\")+1:]
                if(shortName not in processedList):
                    print("processing tweets for twitter file %s" %(metaFile))
                    self.processTweetBatch(metaFile)
                    print("finished processing tweets for twitter file %s" %(metaFile))
                    processedList.append(metaFile[metaFile.rfind("\\")+1:])
                    processedDict = ps.DataFrame({'processedFiles':processedList})
                    processedDict.to_csv(processFile,index=False)

    def getSubdirectories(self,dir):
        return [name for name in os.listdir(dir)
            if os.path.isdir(os.path.join(dir, name))]
    
    def ingestChildProbabilitiesSingleFile(self,data):
        data['tweetId'] = data['tweetId'].astype(str)
        data['isBaby'] = (data['isBaby']*100).astype(int)
        data['isElem'] = (data['isElem']*100).astype(int)
        data['isToddler'] = (data['isToddler']*100).astype(int)
        data['isMiddle'] = (data['isMiddle']*100).astype(int)
        data['isHigh'] = (data['isHigh']*100).astype(int)
        data['isChild'] = (data['isChild']*100).astype(int)   
        result = self.graphDBAPI.processTweetChildProbsBatch(data)

    # given a folder where tweet child probabilities are stored, load files, process, and update Neo4j nodes and relationships
    def ingestChildProbabilitiesFromFolder(self):
        textPredictions = self.predictionsFolder + "age/text_age_pred/"
        hybridPredictions = self.predictionsFolder + "age/hybrid_age_pred/"
        subFolders = self.getSubdirectories(hybridPredictions)
        completedCSV = self.predictionsFolder + "age.csv"
        completed = []
        if os.path.exists(completedCSV):
            completed = ps.read_csv(completedCSV)
            completed = list(completed['completed'])
        for folder in subFolders:
            folderpath = hybridPredictions + folder + "/"
            subsubFolders = os.listdir(folderpath)
            for subsubFolder in subsubFolders:
                subsubFolderpath = folderpath + subsubFolder
                files = os.listdir(subsubFolderpath)
                for file in files:
                    curFilepath = subsubFolderpath + "/" + file
                    if not(curFilepath in completed):
                        hybridPreds = ps.read_csv(curFilepath)
                        hybridPreds['type'] = 0
                        textPreds = ps.read_csv(textPredictions + folder + "/" + subsubFolder + "/" + file)
                        textPreds['type'] = 1
                        df = ps.concat([hybridPreds,textPreds])
                        df.sort_values(by=['type'],ascending=True,inplace=True)
                        df.drop_duplicates(subset=['tweetId'],inplace=True,keep='first')
                        self.ingestChildProbabilitiesSingleFile(df)
                        completed.append(subsubFolderpath + "/" + file)
                        df = ps.DataFrame({
                            'completed':completed
                        }).to_csv(completedCSV,index=False)

        
    def ingestHealthProbabilitiesSingleFile(self,data):
        data['tweetId'] = data['tweetId'].astype(str)
        data['isEmotional'] = (data['isEmotional']*100).astype(int)
        data['isCognitive'] = (data['isCognitive']*100).astype(int)
        data['isPhysical'] = (data['isPhysical']*100).astype(int)
        data['isPositive'] = (data['isPositive']*100).astype(int)
        data['isNegative'] = (data['isNegative']*100).astype(int)
        result = self.graphDBAPI.processTweetHealthProbsBatch(data)

    # given a folder where tweet health probabilities are stored, load files, process, and update Neo4j nodes and relationships
    def ingestHealthProbabilitiesFromFolder(self):
        textPredictions = self.predictionsFolder + "health/text_health_pred/"
        subFolders = self.getSubdirectories(textPredictions)
        completedCSV = self.predictionsFolder + "health.csv"
        completed = []
        index=0
        if os.path.exists(completedCSV):
            completed = ps.read_csv(completedCSV)
            completed = list(completed['completed'])
        for folder in subFolders:
            folderpath = textPredictions + folder + "/"
            subsubFolders = os.listdir(folderpath)
            for subsubFolder in subsubFolders:
                subsubFolderpath = folderpath + subsubFolder
                files = os.listdir(subsubFolderpath)
                for file in files:
                    curFilepath = subsubFolderpath + "/" + file
                    if not(curFilepath in completed):
                        textPreds = ps.read_csv(curFilepath)
                        self.ingestHealthProbabilitiesSingleFile(textPreds)
                        completed.append(subsubFolderpath + "/" + file)
                        index+=1
                        if(index%10==0):
                            ps.DataFrame({
                                'completed':completed
                            }).to_csv(completedCSV,index=False)


    def ingestPlaceProbabilitiesSingleFile(self,data):
        data['tweetId'] = data['tweetId'].astype(str)
        data['isDaycare'] = (data['isDaycare']*100).astype(int)
        data['isPark'] = (data['isPark']*100).astype(int)
        data['isHome'] = (data['isHome']*100).astype(int)
        data['isSchool'] = (data['isSchool']*100).astype(int)
        data['isNeighborhood'] = (data['isNeighborhood']*100).astype(int)
        data['isIndoor'] = (data['isIndoor']*100).astype(int)
        data['isOutdoor'] = (data['isOutdoor']*100).astype(int)
        result = self.graphDBAPI.processTweetPlaceProbsBatch(data)


    # given a folder where tweet child probabilities are stored, load files, process, and update Neo4j nodes and relationships
    def ingestPlaceProbabilitiesFromFolder(self):
        textPredictions = self.predictionsFolder + "place/text_place_pred/"
        hybridPredictions = self.predictionsFolder + "place/hybrid_place_pred/"
        subFolders = self.getSubdirectories(hybridPredictions)
        completedCSV = self.predictionsFolder + "place.csv"
        completed = []
        index=0
        if os.path.exists(completedCSV):
            completed = ps.read_csv(completedCSV)
            completed = list(completed['completed'])
        for folder in subFolders:
            folderpath = hybridPredictions + folder + "/"
            subsubFolders = os.listdir(folderpath)
            for subsubFolder in subsubFolders:
                subsubFolderpath = folderpath + subsubFolder
                files = os.listdir(subsubFolderpath)
                for file in files:
                    curFilepath = subsubFolderpath + "/" + file
                    if not(curFilepath in completed):
                        hybridPreds = ps.read_csv(curFilepath)
                        hybridPreds['type'] = 0
                        textPreds = ps.read_csv(textPredictions + folder + "/" + subsubFolder + "/" + file)
                        textPreds['type'] = 1
                        df = ps.concat([hybridPreds,textPreds])
                        df.sort_values(by=['type'],ascending=True,inplace=True)
                        df.drop_duplicates(subset=['tweetId'],inplace=True,keep='first')
                        self.ingestPlaceProbabilitiesSingleFile(df)
                        completed.append(subsubFolderpath + "/" + file)
                        if(index%10==0):
                            df = ps.DataFrame({
                                'completed':completed
                            }).to_csv(completedCSV,index=False)
                        index+=1


    def processMentionWeights(self,mentionWeights):
        self.graphDBAPI.processMentionWeights(mentionWeights);
    
    def processMentionChildWeight(self,mentionWeights):
        self.graphDBAPI.processMentionChildWeight(mentionWeights);
    
    def processMentions(self,mentionWeights):
        self.graphDBAPI.processMentions(mentionWeights);
    
    def processNChildPosts(self,nChildPosts):
        self.graphDBAPI.processNChildPosts(nChildPosts);
    
    def processNPlacePosts(self,nPlacePosts):
        self.graphDBAPI.processNPlacePosts(nPlacePosts);
    
    def processNHealthPosts(self,nPlacePosts):
        self.graphDBAPI.processNHealthPosts(nPlacePosts);

    # given a folder filled with metadata about images, load metadata and download images to
    # a hashed folder location
    # INPUTS:
    #     metaFolder (str) - absolute filepath to folder containing metadata files
    def copyImagesToNewMetaFolder(self,metaFolder):
        self.tweetImgProcesser.copyImagesFromMetaFolder(metaFolder)

    

    # get place nodes that are missing essential information (e.g. name, bounding box)
    # OUTPUTS:
    #    orphanids (str array) - list of place ids missing information
    def getOrphanPlaceIds(self):
        orphanIds = self.graphDBAPI.getOrphanPlaceIds()
        return orphanIds

    # query Twitter API for place ids that are missing essential information
    def collectPlaceInfo(self):
        orphanPlaceIds = self.graphDBAPI.getOrphanPlaceIds()
        print("found %i Twitter places that need properties " %(len(orphanPlaceIds)))
        self.twitterAPI.downloadPlaceSetJson(orphanPlaceIds)

    # attachments are appendices to the main tweet search return.  Contain extra information,
    # including the media keys (unique ids for each media object) attached to tweets
    # this function parses attachment metadata and extracts the media keys
    # INPUTS:
    #    tweet (dict) - contains tweet information, including attachments
    # OUTPUTS:
    #    media keys (str array) - list of media key ids
    def processAttachments(self,tweet):
        if('attachments' not in tweet.keys()):
            return ['None']
        else:
            return(tweet['attachments']['media_keys'])

    # extract hashtags and user mentions from entities field. See the following for more information
    # about Twitter API entities: 
    # https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/entities
    # INPUTS:
    #    tweet (dict) - contains one tweet and accompanying metadata, including entities
    # OUTPUTS:
    #    mentions (str array) - user mentions in the tweet
    #    hashtags (str array) - hashtags in the tweet
    def processEntities(self,tweet):

        # initialize defaults to return if no mentions or hashtags are avilable to extract
        mentions,hashtags = [],["None"]
        if('entities' not in tweet.keys()):
            return mentions,hashtags
        entityDict = tweet['entities'].keys()

        # extract mentions from tweet record
        if('mentions' in entityDict):
            mentions = []
            for mentionedUser in tweet['entities']['mentions']:
                mentions.append({'twitter_id': tweet['id'],'user_id':mentionedUser['id']})

        # extract hashtags from tweet record
        if('hashtags' in entityDict):
            hashtags = []
            for hashtag in tweet['entities']['hashtags']:
                hashtags.append(hashtag['tag'])

        return(mentions,hashtags)

    # check if a tweet contains a Twitter place object or coordinates. 
    # extract geo information if avilable
    # INPUTS:
    #    tweet (dict) - one tweet and accompanying metadata, including geo infor
    # OUTPUTS:
    #    coords (float array) - lat/lon coords if avilable, ["None","None"] otherwise
    #    placeId (str array) - Twitter place id if available, "None" otherwise
    def processGeo(self,tweet):

        # initialize defaults to return if no goe information is available to extract
        coords,placeId = ["None","None"],"None"
        if('geo' not in tweet.keys()):
            return([coords,placeId])
        geo = tweet['geo']
        geoDict = geo.keys()

        # extract lat/lon coordinates from tweet record
        if('coordinates' in geoDict):
            coords = geo['coordinates']['coordinates']

        # extract place id from tweet record
        if('place_id' in geoDict):
            placeId = geo['place_id']

        return([coords,placeId])

    # for tweets that are referenced within another tweet, extrat these referencces and 
    # reformat to facilitate batch insert into graph DB
    # INPUTS:
    #    tweet (dict) - tweet information, including tweet references
    def processReferencedTweets(self,tweet):

        # return emtpy dict if current tweet does not reference any other tweets
        if('referenced_tweets' not in tweet.keys()):
            return {}
        
        tweetId = tweet['id']
        # initilize defaults if no particular type of reference tweets are found
        retweeted,repliedTo,quoted = [],[],[]

        # for each reference, reformat for batch analysis and add it to the corresponding list
        for reference in tweet['referenced_tweets']:
            valDict = {'twitter_id':tweetId,'ref_id':reference['id']}
            if reference['type'] == 'retweeted':
                retweeted.append(valDict)
            elif reference['type'] == 'replied_to':
                repliedTo.append(valDict)
            elif reference['type'] == 'quoted':
                quoted.append(valDict)

        # combine reference lists into dict
        referenceDict = {
            'retweet':retweeted,
            'reply':repliedTo,
            'quoted_tweet':quoted
        }

        return(referenceDict)

    # TODO: DEPRECATED , verify it is no longer used and then remove
    def processReplyId(self,tweet):
        if('in_reply_to_user_id' not in tweet.keys()):
            return None
        else:
            return(tweet['in_reply_to_user_id'])

    # given references to other tweets, infer if current tweet is original, retweet, reply,
    # or quoted tweet
    # INPUTS:
    #    referenceDict (dict) - contains references to other tweets organized by type 
    # OUTPUTS:
    #     tweet type in str format
    def determineTweetType(self,referenceDict):

        # if there are no references to other tweets, then this is an original tweet
        if(len(referenceDict.keys())==0):
            return('original')
        elif(len(referenceDict['retweet'])>0):
            return('retweet')
        elif(len(referenceDict['reply'])>0):
            return('reply')
        elif(len(referenceDict['quoted_tweet'])>0):
            return('quoted_tweet')

    # given list references for each tweet, combine to create references for a batch
    # of tweets
    # INPUTS:
    #    tweetBatch (dict array) - list of tweet references, one dict for each tweet record
    # OUTPUTS:
    #    single dict containing references for the entire tweet batch
    def reformatReferences(self,tweetBatch):

        # initialize defaults for when no references for a particular type are available 
        replySets,quoteSets,retweetSets = [],[],[]

        # for references of each tweet record, extract refernces and add to an array for the 
        # entire tweet batch
        for tweet in tweetBatch:
            if('reply' in tweet['referenceDict'].keys()):
                replySets += tweet['referenceDict']['reply']
            if('quoted_tweet' in tweet['referenceDict'].keys()):
                quoteSets += tweet['referenceDict']['quoted_tweet']
            if('retweet' in tweet['referenceDict'].keys()):
                retweetSets += tweet['referenceDict']['retweet']

        # create a dict representing references for the entire tweet batch
        return({
            'replies':replySets,
            'quotes':quoteSets,
            'retweets':retweetSets
        })
            
    # reformat a tweet from json to dictonaary format
    # INPUTS:
    #    tweet (json) - tweet in json format
    # OUTPUTS:
    #    tweet in dict format
    def reformatTweet(self,tweet):
        mediaKeys = self.processAttachments(tweet)
        mentions,hashtags = self.processEntities(tweet)
        coords,placeId = self.processGeo(tweet)
        referenceDict = self.processReferencedTweets(tweet)
        tweetType = self.determineTweetType(referenceDict)
        try:
            tweetRecord = {
                'twitter_id':str(tweet['id']),
                'orig_text':tweet['text'],
                'created_at_utc':tweet['created_at'],
                'hashtags':hashtags,
                'media_keys':mediaKeys,
                'coords':coords,
                'place':placeId,
                'tweet_type':tweetType,
                'conv_id':str(tweet['conversation_id']),
                'author_id':str(tweet['author_id']),
                'referenceDict':referenceDict,
                'mentions':mentions
            }   
        except:
            tweetRecord = []
        return(tweetRecord)

    # extract mentions from tweet data
    # INPUTS:
    #    reformattedTweets (list of dicts) - one dict for each tweet
    # OUTPUTS:
    #    list of user mentions
    def reformatMentions(self,reformatedTweets):
        mentions = []
        for tweet in reformatedTweets:
            mentions += tweet['mentions']
        return(mentions)

    # given npy file of tweets, insert into Neo4j database
    # INPUTS:
    #    tweetFile (str) - absolute filepath to npy file containing tweets
    def processTweetBatch(self,tweetFile):
        tweetArr = np.load(tweetFile,allow_pickle=True)
        reformatedTweets = []
        for tweetSubset in tweetArr:
            
            tempTweet = list(map(self.reformatTweet,tweetSubset))
            tempTweet2 = [ele for ele in tempTweet if ele != []]
            reformatedTweets += tempTweet2
        print("%i tweets in file %s" %(len(reformatedTweets),tweetFile))
        index = 0
        multiplier = 100000
        while(index*multiplier < len(reformatedTweets)):
            tweetSusbet = reformatedTweets[index*multiplier:(index+1)*multiplier]
            reformatedReferences = self.reformatReferences(tweetSusbet)
            reformatedMentions = self.reformatMentions(tweetSusbet)
            self.graphDBAPI.processTweetDownloadBatch(tweetSusbet,reformatedReferences,reformatedMentions)
            index+=1
            if(index>1):
                print("completed tweet subset batch %i" %(index))

    
    # given a folder where tweet metafiles are stored, load metafiles, process, and store in Neo4j
    # INPUTS: 
    #    folder (str) - absolute filepath where image metafiles are located
    def ingestTweetsFromFolder(self,kw):
        folder = self.tweetFolder + kw + "/"
        metaFiles = glob.glob(folder + "tw_*")
        processedList = list(ps.read_csv(folder + "processed_list.csv")['processedFiles'])
        print("found %i twitter files " %(len(metaFiles)))
        for metaFile in metaFiles:
            shortName = metaFile[metaFile.rfind("\\")+1:]
            if(shortName not in processedList):
                print("processing tweets for twitter file %s" %(metaFile))
                self.processTweetBatch(metaFile)
                print("finished processing tweets for twitter file %s" %(metaFile))
                processedList.append(metaFile[metaFile.rfind("\\")+1:])
                processedDict = ps.DataFrame({'processedFiles':processedList})
                processedDict.to_csv(folder + "processed_list.csv",index=False)


    # given a folder where tweet metafiles are stored, load metafiles, process, and store in Neo4j
    def ingestUserTweetsFromFolder(self):
        folder = self.userTweetsFolder
        metaFiles = glob.glob(folder + "tw_*")
        processedList = list(ps.read_csv(folder + "processed_list.csv")['processedFiles'])
        print("found %i twitter files " %(len(metaFiles)))
        for metaFile in metaFiles:
            shortName = metaFile[metaFile.rfind("\\")+1:]
            if(shortName not in processedList):
                print("processing tweets for twitter file %s" %(metaFile))
                self.processTweetBatch(metaFile)
                print("finished processing tweets for twitter file %s" %(metaFile))
                processedList.append(metaFile[metaFile.rfind("\\")+1:])
                processedDict = ps.DataFrame({'processedFiles':processedList})
                processedDict.to_csv(folder + "processed_list.csv",index=False)

    # given a folder where tweet metafiles are stored, load metafiles, process, and store in Neo4j
    def ingestConversationTweetsFromFolder(self):
        folder = self.conversationTweetFolder
        metaFiles = glob.glob(folder + "tw_*")
        processedList = list(ps.read_csv(folder + "processed_list.csv")['processedFiles'])
        print("found %i twitter files " %(len(metaFiles)))
        for metaFile in metaFiles:
            shortName = metaFile[metaFile.rfind("\\")+1:]
            if(shortName not in processedList):
                print("processing tweets for twitter file %s" %(metaFile))
                self.processTweetBatch(metaFile)
                print("finished processing tweets for twitter file %s" %(metaFile))
                processedList.append(metaFile[metaFile.rfind("\\")+1:])
                processedDict = ps.DataFrame({'processedFiles':processedList})
                processedDict.to_csv(folder + "processed_list.csv",index=False)


    # given a folder where tweet metafiles are stored, load metafiles, process, and store in Neo4j
    def ingestConversationTweetsFromHashFolder(self):
        folder = self.conversationTweetFolder
        processedListFolder = folder + "processed/"
        for subfolder in range(5000):
            curFolder = folder + str(subfolder) + "/"
            metaFiles = glob.glob(curFolder + "tw_*")
            processFile = processedListFolder + "processed_list_" + str(subfolder) + ".csv"
            processedList = list(ps.read_csv(processFile)['processedFiles'])
            print("found %i twitter files for subfolder %i" %(len(metaFiles),subfolder))
            for metaFile in metaFiles:
                shortName = metaFile[metaFile.rfind("\\")+1:]
                if(shortName not in processedList):
                    print("processing tweets for twitter file %s" %(metaFile))
                    self.processTweetBatch(metaFile)
                    print("finished processing tweets for twitter file %s" %(metaFile))
                    processedList.append(metaFile[metaFile.rfind("\\")+1:])
                    processedDict = ps.DataFrame({'processedFiles':processedList})
                    processedDict.to_csv(processFile,index=False)

    # given a folder where tweet metafiles are stored, load metafiles, process, and store in Neo4j
    def getConversationWordCounts(self):
        folder = self.conversationTweetFolder
        metaFiles = glob.glob(folder + "tw_*")
        wordCounts = ps.read_csv(folder + "converation_counts.csv")
        processedList = list(wordCounts['convId'])
        processedCounts = list(wordCounts['count'])
        print("found %i twitter files " %(len(metaFiles)))
        for metaFile in metaFiles[0:10]:
            shortName = metaFile[metaFile.rfind("\\")+1:]
            if(shortName not in processedList):
                print("counting tweets for twitter file %s" %(metaFile))
                tweetCount = self.countTweetsInFile(metaFile)
                processedCounts.append(tweetCount)
                processedList.append(shortName)
        df = ps.DataFrame({
            'convId':processedList,
            'count':processedCounts
        })
        df.to_csv(folder + "converation_counts.csv",index=False)

    # given file with info for a single Twitter place, load place data into Neo4j database
    # INPUTS:
    #    placeFile (str) - absolute filepath to file containing place data
    def ingestPlaceFromFile(self,placeFile,placeId):
        f = open(placeFile,encoding='utf-8')
        placeData = json.load(f)
        f.close()
        try:
            tempCoords = placeData['bounding_box']['coordinates'][0]
        except Exception as e:
            print("couldn't ingest place for file: %s: %s" %(placeFile,str(e)))
            try:
                self.graphDBAPI.placeDriver.addPlaceError(placeId)
                print("inserted error for place %s" %(placeId))
            except Exception as e:
                print("couldn't insert place error msg: %s" %(str(e)))
            return
        bboxLat,bboxLon = [],[]
        for coord in tempCoords:
            bboxLat.append(coord[1])
            bboxLon.append(coord[0])
        placeData['bounding_box']['lat'] = bboxLat
        placeData['bounding_box']['lon'] = bboxLon
        try:
            self.graphDBAPI.placeDriver.addPlaceInfo(placeData)
        except Exception as e:
            print("couldn't load place information for file: %s: %s" %(placeFile,str(e)))
        print("completed place %s" %(placeFile))

    # given folder containing all place .json files, load place info into Neo4j database
    def ingestPlacesFromFolder(self):
        placeFiles = glob.glob(self.placeFolder + "*.json")
        placeIdsToUpdate = self.graphDBAPI.getOrphanPlaceIds()
        print("found %i places" %(len(placeFiles)))
        index = 0
        for placeFile in placeFiles:
            fileId = placeFile[placeFile.rfind("\\")+1:-5]
            if(fileId in placeIdsToUpdate):
                try:
                    self.ingestPlaceFromFile(placeFile,fileId)
                except Exception as e:
                    print("couldn't ingest place from file %s: %s" %(placeFile,str(e)))
            index +=1
            if(index%10000==0):
                print(index)

    # reformat user node data into dict 
    # INPUTS: 
    #    rawUserData (list of dicts) - one dict for each user node record
    # OUTPUTS:
    #    reformatedUserData (list of dicts) - one dict for each user node records,
    #                                         reformatted 
    def reformatUserData(self,rawUserData):
        reformatedUserData = []
        for curUserData in rawUserData:
            if('location' in curUserData.keys()):
                location = curUserData['location']
            else:
                location = "None"
            curRecord = {
                'id':curUserData['id'],
                'username':curUserData['username'],
                'created_at_utc':curUserData['created_at'],
                'location':location
            }
            reformatedUserData.append(curRecord)
        return(reformatedUserData)

    # load user node info from npy and ingest into Neo4j database
    # INPUTS:
    #    dataFilepath (str) - absolute filepath to npy file containing user info
    def ingestUsersFromFile(self,dataFilepath):
        rawUserData = np.load(dataFilepath,allow_pickle=True)
        reformatedUserData = self.reformatUserData(rawUserData)
        self.graphDBAPI.processUserInfoBatch(reformatedUserData)

    # look for users with missing data and download data from Twitter
    def downloadMissingUserData(self):
        missingUserIds = self.graphDBAPI.getOrphanUsers()
        self.twitterAPI.getTwitterUserInfo(missingUserIds)
        dataFilepath = self.userFolder + self.twitterAPI.hashKey(missingUserIds[0]) + "/" + missingUserIds[0] + ".npy"
        self.ingestUsersFromFile(dataFilepath)

    # download one year of Twitter data for a specific category
    # INPUTS:
    #    year (int) - year of interest
    #    kwType (str) - category of interest
    def downloadTwitterDataOneYear(self,year,kwType):
        self.twitterAPI.processSingleTwitterOneYear(year,kwType)
        return 0

    # insert Community nodes with associated properties.  Does not add relationships between nodes
    # INPUTS:
    #    commData (pandas df) - 
    def insertCommunities(self,commData):
        self.graphDBAPI.insertComm(commData)
        return 0

        
### End of TweetIngestClass.py