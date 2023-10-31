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
        self.placeFolder = storageFolder + "PlaceStore/"
        self.userFolder = storageFolder + "UserStore/"
        self.keywordFolder = storageFolder + "keyword_csvs/"
        self.tweetImgProcesser = TweetImage(self.imageFolder)

        # setup interacting with the API
        self.API_KEY = apiDict['API_KEY']
        self.SECRET = apiDict['SECRET']
        self.twitterAPI = TwitterAPI(
            self.API_KEY,self.SECRET,self.imageFolder,self.tweetFolder,
            self.keywordFolder,self.placeFolder,self.userFolder
        )

        # setup connection to Neo4j database.  This is needed for both database development
        # and analysis
        self.graphDBAPI = GraphDAO(dbDict['dbUri'],dbDict['dbUser'],dbDict['dbPW'])

    # given a folder filled with metadata about images, load metadata and download images to
    # a hashed folder location
    # INPUTS:
    #     metaFolder (str) - absolute filepath to folder containing metadata files
    def downloadImagesFromMetaFolder(self,metaFolder):
        self.tweetImgProcesser.downloadImagesFromMetaFolder(metaFolder)

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
        reformatedReferences = self.reformatReferences(reformatedTweets)
        reformatedMentions = self.reformatMentions(reformatedTweets)
        self.graphDBAPI.processTweetDownloadBatch(reformatedTweets,reformatedReferences,reformatedMentions)

    
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

    # given file with info for a single Twitter place, load place data into Neo4j database
    # INPUTS:
    #    placeFile (str) - absolute filepath to file containing place data
    def ingestPlaceFromFile(self,placeFile):
        f = open(placeFile)
        placeData = json.load(f)
        try:
            tempCoords = placeData['bounding_box']['coordinates'][0]
        except Exception as e:
            print("couldn't ingest place for file: %s: %s" %(placeFile,str(e)))
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
        f.close()

    # given folder containing all place .json files, load place info into Neo4j database
    def ingestPlacesFromFolder(self):
        placeFiles = glob.glob(self.placeFolder + "*.json")
        placeIdsToUpdate = self.graphDBAPI.getOrphanPlaceIds()
        print("found %i places" %(len(placeFiles)))
        for placeFile in placeFiles:
            fileId = placeFile[placeFile.rfind("\\")+1:-5]
            if(fileId in placeIdsToUpdate):
                self.ingestPlaceFromFile(placeFile)

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



        
### End of TweetIngestClass.py