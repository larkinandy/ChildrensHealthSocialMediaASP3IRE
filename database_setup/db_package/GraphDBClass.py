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

# end of GraphDBClass.py