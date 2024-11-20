### TweetNodeClass.py
### Author: Andrew Larkin
### Date Created: May 17, 2022
### Summary: Class constructing and querying Tweet nodes in a Neo4j database

import pandas as ps

class TweetDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver,debug=False):
        self.driver=driver
        self.debug=debug

    # code for creating relationships between new tweet nodes and user nodes.
    # runs in batch mode, i.e. performing many updates per commit
    # OUTPUTS:
    #    code (str) - code block used as part of a transaction for creating new nodes
    def createCodeForTweetUserRelationshipsApoc(self):
        code = """
            CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
            "MATCH(t:Tweet{id:label.twitter_id})
            MERGE (a:TwitterUser {id:label.author_id})
            with t,a
            MATCH (s:Analyzed {type:'stale'})
            MERGE (a)-[:IN_STAGE]->(s)
            MERGE (t)<-[:POSTED]-(a)",
            {batchSize:1000,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        return code

    # code for creating placce nodes and relationships between places and tweets 
    # runs in batch mode, i.e. performing many updates per commit
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for creating new nodes
    def setTweetPlaceApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:Tweet {id:label.twitter_id})
        MERGE (p:TwitterPlace {id:label.place})
        with t,p
        MERGE (t)-[:IN_PLACE]->(p)",
        {batchSize:1000,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        return code

    # code for deleting relationships that don't exist.  Needed to batch process tweet nodes in parallel
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for deleting relationships and the 'None' place node
    def cleanTweetPlace(self):
        code = """
        MATCH(p:TwitterPlace{id:'None'})
        DETACH DELETE p
        """
        return code

    # code for creating tweet nodes and setting tweet properties.  Does not create relationships to other nodes so it 
    # can run in parallel batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for creating tweet nodes and properties
    def setTweetNodePropertiesApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MERGE (t:Tweet {id:label.twitter_id})
            SET t.orig_text= label.orig_text
            SET t.created_at_utc=label.created_at_utc
            SET t.hashtags=label.hashtags
            SET t.media_keys= label.media_keys
            SET t.coords=label.coords",
        {batchSize:500,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code

    # code for creating relationships between tweet and tweet type
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for linking tweet nodes and tweet type
    def setTweetNodeLabelApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:Tweet {id:label.twitter_id})
        MATCH (tt:TweetType {type:label.tweet_type})
        MERGE (t)-[:IS_TYPE]->(tt)
        WITH t
            OPTIONAL MATCH (t)-[r:IN_STAGE]->()
            DELETE r
            WITH t
            MATCH (a2:Analyzed {type:'downloaded'})
            MERGE (t)-[:IN_STAGE]->(a2)",
        {batchSize:1000,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        return code
    
    # code for adding child label probabilities to tweets
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for adding child probablities as a property to tweet nodes
    def setTweetChildProbsApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:Tweet {id:label.tweetId})
        SET t.isBaby = label.isBaby,
        t.isToddler = label.isToddler,
        t.isElem = label.isElem,
        t.isMiddle = label.isMiddle,
        t.isHigh = label.isHigh,
        t.isChild = label.isChild",
        {batchSize:1000,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        return code
    
    # code for adding health label probabilities to tweets
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for adding child probablities as a property to tweet nodes
    def setTweetHealthProbsApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:Tweet {id:label.tweetId})
        SET t.isCognitive = label.isCognitive,
        t.isEmotionalSocial = label.isEmotional,
        t.isPhysical = label.isPhysical,
        t.isNegative = label.isNegative,
        t.isPositive = label.isPositive",
        {batchSize:1000,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        return code
    
    # code for adding place label probabilities to tweets
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for adding place probablities as a property to tweet nodes
    def setTweetPlaceProbs(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:Tweet {id:label.tweetId})
        SET t.isDaycare = label.isDaycare,
        t.isPark = label.isPark,
        t.isHome = label.isHome,
        t.isSchool = label.isSchool,
        t.isNeighborhood = label.isNeighborhood,
        t.isOutdoor = label.isOutdoor,
        t.isIndoor = label.isIndoor",
        {batchSize:10000,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code
    

 
    # create connection between tweet node and conversation node
    # runs in batch mode
    # OUTPUTS:
    #    code (str) - code block used as part of a transactoin for creating a new node
    def setConversationNodeApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:Tweet{id:label.twitter_id})
        MERGE (c:Conversation {id:label.conv_id})
        WITH t,c
        MERGE (t)-[:BELONGS_TO]->(c)
        WITH t,c
        MATCH (a2:Analyzed {type:'stale'})
        MERGE (c)-[:IN_STAGE]->(a2)",
        {batchSize:1000,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        return code

    # create user node - included in node class because part of creating a tweet node is also establishing 
    # relationships with user nodes and, if necessary, creating user nodes
    # INPUTS:
    #    tx (transaction) - open transaction with Neo4j database
    #    tweetId (str) - unique id of tweet to connect user to
    #    userId (str) - unique id user to connect tweet to
    # OUTPUTS:
    #    result (str) - result of the transaction
    def addMentionsApoc(self,tx,mentionsBatch):
        cypher = """
            CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
            "MATCH(t:Tweet {id:label.twitter_id})
            MERGE(u:TwitterUser {id:label.user_id})
            MERGE (t)-[:MENTIONED]->(u)",
            {batchSize:1000,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        result = tx.run(cypher,labels=mentionsBatch)
        if(self.debug):
            print(cypher)
            print("this is the result for mentions")
            records = [record for record in result]
            print(records)
        return(result)

    # if a tweet references another tweet, add a relationship 
    # runs in batch mode.  All tweets in batch must have the same 
    # relationship (e.g. retweet, reply, etc)
    # INPUTS:
    #   tx (transaction) - open transaction with Neo4j database
    #   refBatch (json list) - one json object for each relationship to add
    #   refType (str) - indicates type of relationship to add
    # OUTPUTS:
    #   result (str) -result of the transaction
    def addRetweetReferenceApoc(self,tx,refBatch,refType):
        cypher = """
            CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
            "MATCH(t:Tweet {id:label.twitter_id})
            MERGE(t2:Tweet {id:label.ref_id})
            """
        if(refType=='retweets'):
            cypher += """
                MERGE (t2)<-[:RETWEET]-(t)",
            """
        elif(refType=='quotes'):
            cypher += """
                MERGE (t2)<-[:Q_TWEET]-(t)",
            """
        else:
            cypher += """
                MERGE (t2)<-[:REPLY]-(t)",
            """
        cypher += """
        {batchSize:1000,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        result = tx.run(cypher,labels=refBatch)
        if(self.debug):
            print(cypher)
        return(result)


    # connect tweet node to node in time tree corresponding to the time the tweet was made
    # INPUTS:
    #    timestamp (str) - time tweet was made
    # OUTPUTS:
    #    code (str) - code block used as part of a transaction for creating the relationship
    def addCodeForTimeTree(self,timestamp):
        code = """
            with t
            MATCH(d:Day {day:$day})--(:Month {month:$month})--(:Year {year:$year})
            MERGE (t)-[:POSTED_ON]->(d)
        """
        return(code)

   
    # insert batch of Tweet nodes.  Relationships between tweets and other nodes are created
    # in different functions
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    tweetBatch (json array) - set of tweets, each tweet as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertTweetBatch(self,tx,tweetBatch):
        cypher = self.setTweetNodePropertiesApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result of tweet batch insertion")
            print(cypher)
            records = [record for record in result]
            print(records)
        return(result)
       
    # update Tweet nodes with child age group probabilities.  
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    probabilities (json array) - set of tweet probabilities, probabilities for each tweet as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertTweetChildProbsBatch(self,tx,tweetBatch):
        cypher = self.setTweetChildProbsApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result of tweet batch insertion")
            print(cypher)
            records = [record for record in result]
            print(records)
        return(result)
    

    # update Tweet nodes with child health probabilities.  
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    probabilities (json array) - set of tweet probabilities, probabilities for each tweet as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertTweetHealthProbsBatch(self,tx,tweetBatch):
        cypher = self.setTweetHealthProbsApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result of tweet batch insertion")
            print(cypher)
            records = [record for record in result]
            print(records)
        return(result)

    # create relationships between a batch of tweets and their type (e.g. retweet)
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    tweetBatch (json array) - set of tweets, each tweet as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertTweetTypeBatch(self,tx,tweetBatch):
        cypher = self.setTweetNodeLabelApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result for tweet type")
            records = [record for record in result]
            print(records)
        return(result)

    # update Tweet nodes with child place probabilities.  
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    probabilities (json array) - set of tweet probabilities, probabilities for each tweet as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertTweetPlaceProbsBatch(self,tx,tweetBatch):
        cypher = self.setTweetPlaceProbs()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result for tweet type")
            records = [record for record in result]
            print(records)
        return(result)


    # insert relationship between tweet node and TwitterPlace
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    tweetBatch (json array) - set of tweets, each tweet as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertTweetPlaceBatch(self,tx,tweetBatch):
        cypher = self.setTweetPlaceApoc()
        cypher2 = self.cleanTweetPlace()
        try:
            result = tx.run(cypher,labels=tweetBatch)
            result2 = tx.run(cypher2)
            if(self.debug):
                print("results for inserting tweet place relationships")
                print(cypher)
            return(result)
        except Exception as e:
            print(str(e))
            return None

    # create a relationship between tweet node and conversation thread id
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    tweetBatch (json array) - set of tweets, each tweet as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertTweetConversationBatch(self,tx,tweetBatch):
        cypher = self.setConversationNodeApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
            if(self.debug):
                print("this is the result for conversation node")
                print(cypher)
                records = [record for record in result]
                print(records)
            return(result)
        except Exception as e:
            print(str(e))
            return None

    # create relationships between tweet nodes and authors
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    tweetBatch (json array) - set of tweets, each tweet as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertTweetAuthorBatch(self,tx,tweetBatch):
        cypher = self.createCodeForTweetUserRelationshipsApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
            if(self.debug):
                print("result of inserting tweet authors and relationships")
                print(cypher)
            return(result)
        except Exception as e:
            print(str(e))
            return None

    # given a tweet, create tweet node, relationships between tweet and users, and relationships between tweet and referenced tweets
    # INPUTS:
    #    tweetList (json array) - list of tweets, where each tweet is an independent json object
    #    refTweets (dict) - values for each key corresponding to json arrays.  One array each retweets, quotes, and replies
    #    userMentions (json array) - list of {tweet,user_id} objects for all users @ in a tweet
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processTweetDownloadBatch(self,tweetList,refTweets,userMentions):
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.insertTweetBatch,tweetList)
                result2 = session.write_transaction(self.insertTweetTypeBatch,tweetList)
                result3 = session.write_transaction(self.insertTweetPlaceBatch,tweetList)
                result4 = session.write_transaction(self.insertTweetConversationBatch,tweetList)
                result5 = session.write_transaction(self.insertTweetAuthorBatch,tweetList)
                for kw in ['replies','quotes','retweets']:
                    if(kw in refTweets.keys()):
                        tempResult = session.write_transaction(self.addRetweetReferenceApoc,refTweets[kw],kw)
                result = session.write_transaction(self.addMentionsApoc,userMentions)
                print("completedBatch")
                return result
            except Exception as e:
                return e

    # update tweet nodes with probabilities they are related to child age group. 
    # INPUTS:
    #    childProbs (pandas df) - tweet ids and probabilities for each child age group
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processTweetChildProbs(self,childProbs):
        jsonData = list(childProbs.apply(lambda x: x.to_dict(), axis=1))
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.insertTweetChildProbsBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e

    # update tweet nodes with probabilities they are related to health symptoms/outcomes. 
    # INPUTS:
    #    healthProbs (pandas df) - tweet ids and probabilities for each health category
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processTweetHealthProbs(self,healthProbs):
        jsonData = list(healthProbs.apply(lambda x: x.to_dict(), axis=1))
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.insertTweetHealthProbsBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e
            
    # update tweet nodes with probabilities they are related to places.  
    # INPUTS:
    #    placeProbs (pandas df) - tweet ids and probabilities for each place category
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processTweetPlaceProbs(self,placeProbs):
        jsonData = list(placeProbs.apply(lambda x: x.to_dict(), axis=1))
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.insertTweetPlaceProbsBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e
    
    
    # select users who have a relationship with the (:Analyzed {type:'orphan'}) node.  These are
    # users who have been identified as invovled in a tweet, but whose meta information has not yet
    # been collected
    def selectUnknownTweets(self):
        def inLineFx(tx):
            query = """
                MATCH (t:Tweet) WHERE t.text IS NULL
                RETURN t.id
            """
            result=tx.run(query)
            userIds = [row.values()[0] for row in result]
            return(userIds)

        with self.driver.session() as session:
            result = session.read_transaction(inLineFx)
            return result
        
    def getUsernameFromTweetId(self,tweetId):
        def inLineFx(tx,tweetId):
            query = """
                MATCH (t:Tweet{id:'""" + str(tweetId) + """'})<-[p:POSTED]-(n)
                RETURN n.username
            """
            result=tx.run(query,tweetId=tweetId)
            userId = [row.values()[0] for row in result][0]
            return(userId)

        with self.driver.session() as session:
            result = session.read_transaction(inLineFx,tweetId)
            return result
        

    def getConvIdFromTweetId(self,tweetId):
        def inLineFx(tx,tweetId):
            query = """
                MATCH (t:Tweet{id:'""" + str(tweetId) + """'})-[b:BELONGS_TO]->(c)
                RETURN c.id
            """
            result=tx.run(query,tweetId=tweetId)
            convId = [row.values()[0] for row in result][0]
            return(convId)

        with self.driver.session() as session:
            result = session.read_transaction(inLineFx,tweetId)
            return result


    # county number of tweets which contain a keyword
    # INPUTS:
    #    kw (str) - keyword of interest
    # OUTPUTS:
    #    number of imtes keyword is found
    def countKeywordIncidence(self,kw):
        def inLineFxn(tx,kw):
            kw = kw.replace(" ", " AND ")
            query = """
                CALL db.index.fulltext.queryNodes('twtxt', "'""" + str(kw) + """'") YIELD node RETURN count(node);
            """
            result = tx.run(query)
            count = [row.values()[0] for row in result]
            return(count[0])

        with self.driver.session() as session:
            result = session.read_transaction(inLineFxn,kw)
            return result

    # randomly sample tweets containing a keyword of interest
    # INPUTS:
    #    kw (str) - keyword of interest
    #    sampSize (int) - random sample size
    # OUTPUTS:
    #    list of randomly sampled tweets
    def getKeywordRandomSample(self,kw,sampleSize=100):
        def inLineFxn(tx,kw):
            kw = kw.replace(" ", " AND ")
            query = """
                CALL db.index.fulltext.queryNodes('twtxt', "'""" + str(kw) + """'") YIELD node
                WITH node, rand() AS number
                RETURN {text:node.orig_text,id:node.id,media_keys:node.media_keys,created_at:node.created_at_utc}
                ORDER BY number 
                LIMIT """ + str(sampleSize)
            result = tx.run(query)
            count = [row.values()[0] for row in result]
            return(count)

        with self.driver.session() as session:
            result = session.read_transaction(inLineFxn,kw)
            return result
        

    def getTweetsByUser(self,tweetId):
        def inLineFx(tx,tweetId):
            query = """
                MATCH (u:TwitterUser{username:'""" + str(tweetId) + """'})-[p:POSTED]->(t:Tweet)
                RETURN t.orig_text
            """
            print(tweetId)
            result=tx.run(query,tweetId=tweetId)
            print(result)
            tweets = [row.values()[0] for row in result]
            print(tweets)
            return(tweets)

        with self.driver.session() as session:
            result = session.read_transaction(inLineFx,tweetId)
            return result

# end of TweetNodeClass.py