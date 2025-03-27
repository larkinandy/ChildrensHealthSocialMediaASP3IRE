### GraphDBClass.py
### Author: Andrew Larkin
### Date Created: May 18, 2022
### Summary: Perform all operations needed to interact with Twitter Neo4j database


from neo4j import GraphDatabase
from datetime import datetime
from TweetNodeClass import TweetDAO
from TimeNodeClass import TimeDAO
from UserNodeClass import UserDAO
from ConversationNodeClass import ConversationDAO
from LabelNodeClass import LabelDAO
from GeoNodeClass import GeoDAO
from TweetPlaceClass import TweetPlaceDAO
from CommunityClass import CommunityDAO


class GraphDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, uri,username,password):
        self.driver=self.init_driver(uri,username,password)
        self.tweetDriver = TweetDAO(self.driver)
        self.timeDriver = TimeDAO(self.driver)
        self.userDriver = UserDAO(self.driver)
        self.conversationDriver = ConversationDAO(self.driver)
        self.labelDriver = LabelDAO(self.driver)
        self.geoDriver = GeoDAO(self.driver)
        self.placeDriver = TweetPlaceDAO(self.driver)
        self.commDriver = CommunityDAO(self.driver)

    # create a connection to the Neo4j database
    # INPUTS:
    #    uri (str) - path to database
    #    username (str) - user to login to database as
    #    password (str) - user password
    # OUTPUTS:
    #    open Twitter Neo4j database connection
    def init_driver(self,uri, username, password):
        # Create an instance of the driver
        driver = GraphDatabase.driver(uri, auth=(username, password))
        # Verify Connectivity
        driver.verify_connectivity()
        return driver

    # given a set of tweets downloaded in .npy format, ingest tweets into Neo4j database
    # INPUTS:
    #    tweetBatch (array) - tweets 
    #    refBatch (array) - reference types (e.g. retweet)
    #    userMentions (array) - when users are mentioned in tweets 
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processTweetDownloadBatch(self,tweetBatch,refBatch,userMentions):
        try:
            result = self.tweetDriver.processTweetDownloadBatch(tweetBatch,refBatch,userMentions)
            return result
        except Exception as e:
            return e
        

    # given a set of probabilities tweets are related to child age groups, add probabilities and relationships to Tweet nodes
    # INPUTS:
    #    tweetBatch (pandasDF) - tweet probabilities 
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processTweetChildProbsBatch(self,tweetBatch):
        try:
            result = self.tweetDriver.processTweetChildProbs(tweetBatch)
            return result
        except Exception as e:
            return e
        
    # given a set of probabilities tweets are related to child age groups, add probabilities and relationships to Tweet nodes
    # INPUTS:
    #    tweetBatch (pandasDF) - tweet probabilities 
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processTweetHealthProbsBatch(self,tweetBatch):
        try:
            result = self.tweetDriver.processTweetHealthProbs(tweetBatch)
            return result
        except Exception as e:
            return e
        
    # given a set of probabilities tweets are related to child age groups, add probabilities and relationships to Tweet nodes
    # INPUTS:
    #    tweetBatch (pandasDF) - tweet probabilities 
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processTweetPlaceProbsBatch(self,tweetBatch):
        try:
            result = self.tweetDriver.processTweetPlaceProbs(tweetBatch)
            return result
        except Exception as e:
            return e
        
    # given a set of weights for MENTION relationships and the two nodes on the ends of the relationship, set the mention weight property
    # INPUTS:
    #    mentionWeights (pandasDF) - ids for the two nodes and the weight for the connecting MENTION relationship
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processMentionWeights(self,tweetBatch):
        try:
            result = self.userDriver.processMentionWeight(tweetBatch)
            return result
        except Exception as e:
            return e
        
    # given a set of weights for MENTION relationships and the two nodes on the ends of the relationship, set the mention weight property
    # INPUTS:
    #    mentionWeights (pandasDF) - ids for the two nodes and the weight for the connecting MENTION relationship
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processMentionLabelWeights(self,tweetBatch):
        try:
            result = self.userDriver.processMentionLabelWeights(tweetBatch)
            return result
        except Exception as e:
            return e
        
    # given a set of mentions for twitterAuthors set the mentions/mentioned properties on twitter author nodes 
    # INPUTS:
    #    mentions (pandasDF) - Twitter author ids and number of mentioned/mentions
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processMentions(self,tweetBatch):
        try:
            result = self.userDriver.processMentions(tweetBatch)
            return result
        except Exception as e:
            return e
        
    # set the number of posts TwitterAuthors made about children
    # INPUTS:
    #    nChildren (pandasDF) - Twitter author ids and number of posts about each child age group
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processNChildPosts(self,nChildren):
        try:
            result = self.userDriver.processNChild(nChildren)
            return result
        except Exception as e:
            return e
        
    # set the number of posts TwitterAuthors made about safe places
    # INPUTS:
    #    nPlaces (pandasDF) - Twitter author ids and number of posts about each child age group
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processNPlacePosts(self,nPlaces):
        try:
            result = self.userDriver.processNPlace(nPlaces)
            return result
        except Exception as e:
            return e

    # set the number of posts TwitterAuthors made about health symptoms/outcomes
    # INPUTS:
    #    nPlaces (pandasDF) - Twitter author ids and number of posts about each child age group
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def processNHealthPosts(self,nHealth):
        try:
            result = self.userDriver.processNHealth(nHealth)
            return result
        except Exception as e:
            return e
    
    
    # get place ids which do not have geo coord data yet
    # OUTPUTS:
    #    list of place ids that do not have info yet
    def getOrphanPlaceIds(self):
        orphanIds = self.placeDriver.getOrphanPlaceIds()
        return orphanIds

    # get userr ids which do not have location info yet
    # OUTPUTS:
    #    list of users thaat do not have info yet
    def getOrphanUsers(self):
        orphanIds = self.userDriver.getOrphanUsers()
        return(orphanIds)

    # insert user info into Neo4j database
    def processUserInfoBatch(self,userBatch):
        self.userDriver.insertUserBatch(userBatch)
    
    # count how many tweets in the database contain a keyword
    def countKeywordIncidence(self,kw):
        count = self.tweetDriver.countKeywordIncidence(kw)
        return(count)

    # randomly sample tweets containing a keyword
    # INPUTS:
    #    kw (str) - keyword to search for
    #    sampSize (int) - number of tweets to randomly sample
    # OUTPUTS:
    #    list of randomly sampled tweets
    def getKeywordRandomSample(self,kw,sampSize):
        randomSample = self.tweetDriver.getKeywordRandomSample(kw,sampSize)
        return(randomSample)

    def getUsernameFromTweetId(self,tweetId):
        username = self.tweetDriver.getUsernameFromTweetId(tweetId)
        return(username)
    
    def getUserIdFromTweetId(self,tweetId):
        username = self.tweetDriver.getUserIdFromTweetId(tweetId)
        return(username)

    def getConvIDFromTweetId(self,tweetId):
        convId = self.tweetDriver.getConvIdFromTweetId(tweetId)
        return(convId)
    

    # given a set of communities in pandas format create community nodes into Neo4j database
    # INPUTS:
    #    commBatch (pandas DF) - communities and associated properties, one comm for each row
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def insertComm(self,commBatch):
        try:
            result = self.commDriver.insertComm(commBatch)
            return result
        except Exception as e:
            return e

    # given a set of communitity:user id parks in pandas, create relationships between communities and user nodes
    # INPUTS:
    #    commRelBatch (pandas DF) - communities and user ids
    # OUTPUTS:
    #    result code or error if tweets cannot be processed
    def insertCommRelationships(self,commRelBatch):
        try:
            result = self.commDriver.insertCommRelationships(commRelBatch)
            return result
        except Exception as e:
            return e

    def getTweetsForUsers(self,userId):
        try:
            result = self.tweetDriver.getTweetsByUser(userId)
            return result
        except Exception as e:
            return e
        
    def insertTweetTopics(self,topicBatch):
        try:
            result = self.tweetDriver.insertTweetTopics(topicBatch)
            return result
        except Exception as e:
            return e
        
    def insertStateGeo(self,stateBatch):
        try:
            result = self.userDriver.setStateGeo(stateBatch)
            return result
        except Exception as e:
            return e
        
    def insertCityGeo(self,cityBatch):
        try:
            result = self.userDriver.setCityGeo(cityBatch)
            return result
        except Exception as e:
            return e


# end of GraphDBClass.py