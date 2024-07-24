# UserNodeClass.py 
# Author: Andrew Larkin
# Summary: Perform operations on Neo4j database related to user nodes
# Part of a set of classes for the ASP3IRE project


class UserDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver=driver


    # select users who have a relationship with the (:Analyzed {type:'orphan'}) node.  These are
    # users who have been identified as invovled in a tweet, but whose meta information has not yet
    # been collected
    def getOrphanUsers(self):
        def inLineFxn(tx):
            #query = """
            #    MATCH (u:TwitterUser)--(:Analyzed {type:'orphan'})
            #    RETURN u.id
            #"""
            query = """
            MATCH(u:TwitterUser) WHERE u.username IS NULL
            RETURN u.id limit 1000
            """
            result=tx.run(query)
            userIds = [row.values()[0] for row in result]
            #print(userIds[0])
            #print(userIds[0].values())
            return(userIds)

        with self.driver.session() as session:
            result = session.read_transaction(inLineFxn)
            return result

    # select users who have a relationship with the (:Analyzed {type:'orphan'}) node.  These are
    # users who have been identified as invovled in a tweet, but whose meta information has not yet
    # been collected
    def insertUser(self,tx,user_id,name,screen_name,created_at,location=None,description=None,city_id=None):
        query = """
        MERGE (u:TwitterUser {id:$user_id})
        with u
        OPTIONAL MATCH (u)-[s:IN_STAGE]->()
        DELETE s
        with u
        SET u.name= $name
        SET u.screen_name = $screen_name
        SET u.created_at = $created_at
        """
        if(location!=None):
            query += """SET u.location = $location"""
        if(description!=None):
            query += """
        SET u.description = $description"""
        # currently thinking we don't want to update all tweets for every user in the orphan category.  Only those who have written
        # a child-related tweet
        #query += """
        #with u
        #MATCH (a:Analyzed {type:'stale'})
        #MERGE (u)-[:IN_STAGE]->(a)"""
        if(city_id!=None):
            query += """
            with u
            MATCH (c:City {id:$city_id})
            MERGE (u)-[:LIVED_IN]->(c)
            """
        result = tx.run(
            query,user_id=user_id,name=name,screen_name=screen_name,created_at=created_at,location=location,description=description,city_id=city_id)
        return(result)
        
    # create code for batch updating user nodes.  Can run in parallel because each record will update 
    # the properties of one and only one node
    # OUTPUTS:
    #    code (str) - cypher statement for batch upsert
    def createBatchUserInsertCode(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MERGE (u:TwitterUser {id:label.id})
            SET u.username= label.username
            SET u.created_at_utc= label.created_at_utc
            SET u.location= label.location",
        {batchSize:500,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code
    
    # code for setting the number of times an author posts a tweet about a safe place(s)
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for setting a relationship property
    def setNPlacesApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:TwitterUser {id:label.tweetId})
        SET t.nPark = label.isPark,
        t.nNeighborhood = label.isNeighborhood,
        t.nSchool = label.isSchool,
        t.nHome = label.isHome,
        t.nDaycare = label.isDaycare,
        t.nOutdoor = label.isOutdoor,
        t.nIndoor = label.isIndoor",
        {batchSize:50000,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code

    # code for setting the number of times an author posts a tweet about a child(ren)
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for setting a relationship property
    def setNChildApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:TwitterUser {id:label.tweetId})
        SET t.nChild = label.isChild,
        t.nBaby = label.isBaby,
        t.nToddler = label.isToddler,
        t.nElementary = label.isElem,
        t.nMiddle = label.isMiddle,
        t.nHigh = label.ishHigh,
        t.nPosted = label.nPosted",
        {batchSize:50000,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code
    
    # code for setting the number of times an author posts a tweet about a health symptom/outcome
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for setting a relationship property
    def setNHealthApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:TwitterUser {id:label.tweetId})
        SET t.nCognitive = label.isCognitive,
        t.nEmotionalSocial = label.isEmotionalSocial,
        t.nPhysical = label.isPhysical,
        t.nPositive = label.isPositive,
        t.nNegative = label.isNegative",
        {batchSize:50000,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code
    
    # code for setting the number of mentions and mentioned properties on a twitter user node  
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for setting a relationship property
    def setMentionsApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (t:TwitterUser {id:label.tweetId})
        SET t.nMentions = label.nMentions,
        t.nMentioned = label.nMentioned",
        {batchSize:10000,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code
    
    # code for setting the number of mentions properties on a mention relationship 
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for setting a relationship property
    def setMentionsWeightApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (a:TwitterUser {id:label.aTweetId})-[m:MENTIONED]->(b:TwitterUser {id:label.bTweetId})
        SET m.n=label.n",
        {batchSize:50000,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        return code

    # code for setting the number of mentions properties on a mention relationship 
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for setting a relationship property
    def setMentionsWeightLabelsApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (a:TwitterUser {id:label.aTweetId})-[m:MENTIONED]->(b:TwitterUser {id:label.bTweetId})
        SET m.nChild=label.isChild,
        m.nBaby=label.isBaby,
        m.nToddler=label.isToddler,
        m.nElem=label.isElem,
        m.nMiddle=label.isMiddle,
        m.nHigh=label.isHigh,
        m.nPark=label.isPark,
        m.nHome=label.isHome,
        m.nSchool=label.isSchool,
        m.nDaycare=label.isDaycare,
        m.nNeighborhood=label.isNeighborhood,
        m.nPhysical=label.isPhysical,
        m.nEmotionalSocial = label.isEmotionalSocial,
        m.nCognitive=label.isCognitive,
        m.nPositive=label.isPositive,
        m.nNegative=label.isNegative",
        {batchSize:50000,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code

    # update number of times authors mention child(ren)  
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    matchData (dict) - a dictionary of authorids,ntweets
    # OUTPUTS:
    #    result (str) - transaction result
    def setNChildBatch(self,tx,tweetBatch):
        cypher = self.setNChildApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result for tweet type")
            records = [record for record in result]
            print(records)
        return(result)
    
    # update number of child tweet properties on TwitterUser nodes
    # INPUTS:
    #    nChildStats (pandas df) - twitterAuthor id and number of posts about children
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processNChild(self,nChildStats):
        jsonData = list(nChildStats.apply(lambda x: x.to_dict(), axis=1))
        print(jsonData[0])
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.setNChildBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e

    # update number of times authors mention safe place(s)
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    matchData (dict) - a dictionary of authorids,ntweets
    # OUTPUTS:
    #    result (str) - transaction result
    def setNPlaceBatch(self,tx,tweetBatch):
        cypher = self.setNPlacesApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result for tweet type")
            records = [record for record in result]
            print(records)
        return(result)

    # update number of place tweet properties on TwitterUser nodes
    # INPUTS:
    #    nPlaceStats (pandas df) - twitterAuthor id and number of posts about safe places
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processNPlace(self,nPlaceStats):
        jsonData = list(nPlaceStats.apply(lambda x: x.to_dict(), axis=1))
        print(jsonData[0])
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.setNPlaceBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e
            
    # update number of times authors mention health symptoms/outcomes
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    matchData (dict) - a dictionary of authorids,ntweets
    # OUTPUTS:
    #    result (str) - transaction result
    def setNHealthBatch(self,tx,tweetBatch):
        cypher = self.setNHealthApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result for tweet type")
            records = [record for record in result]
            print(records)
        return(result)
    
    # update number of health tweet properties on TwitterUser nodes
    # INPUTS:
    #    nHealthStats (pandas df) - twitterAuthor id and number of posts about health symptoms/outcomes
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processNHealth(self,nHealthStats):
        jsonData = list(nHealthStats.apply(lambda x: x.to_dict(), axis=1))
        print(jsonData[0])
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.setNHealthBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e

    # update mention relationships by setting the weight property  
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    matchData (json array) - data needed to find relationships in database and set weights
    # OUTPUTS:
    #    result (str) - transaction result
    def setMentionWeightBatch(self,tx,tweetBatch):
        cypher = self.setMentionsWeightApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result for tweet type")
            records = [record for record in result]
            print(records)
        return(result)
    
    # update mention relationships by setting the weight properties for child tweets
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    matchData (json array) - data needed to find relationships in database and set weights
    # OUTPUTS:
    #    result (str) - transaction result
    def setMentionWeightLabelsBatch(self,tx,tweetBatch):
        cypher = self.setMentionsWeightLabelsApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result for tweet type")
            records = [record for record in result]
            print(records)
        return(result)
    

    # update mention relationships by setting the weight property  
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    matchData (json array) - data needed to find relationships in database and set weights
    # OUTPUTS:
    #    result (str) - transaction result
    def setNMentionsBatch(self,tx,tweetBatch):
        cypher = self.setMentionsApoc()
        try:
            result = tx.run(cypher,labels=tweetBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result for tweet type")
            records = [record for record in result]
            print(records)
        return(result)
    
    # batch insert user nodes.  In reality this is used for updating rather than inserting nodes,
    # as nodes are most often inserted at the same time their first tweet with a relationship is 
    # inserted
    # INPUTS:
    #     userBatch (json array) - list of nodes to update each json object corresponds to one
    #                              and only one user node
    # OUTPUTS:
    #     result (array of custom neo4j objects) - result of the batch upsert transaction
    def insertUserBatch(self,userBatch):
        def inLineFxn(tx):
            code = self.createBatchUserInsertCode()
            results = tx.run(code,labels=userBatch)
            print(code)
            records = [record for record in results]
            print(records)
            return(results)
        with self.driver.session() as session:
            result = session.write_transaction(inLineFxn)
            return result

    # update MENTION relationships between TwitterAuthors with a weight property (n mentions)
    # INPUTS:
    #    mentionWeights (pandas df) - twitterAuthor ids and mention weights
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processMentionWeight(self,mentionWeights):
        jsonData = list(mentionWeights.apply(lambda x: x.to_dict(), axis=1))
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.setMentionWeightBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e
            
    # update MENTION relationships between TwitterAuthors with a weight property (n mentions)
    # INPUTS:
    #    mentionWeights (pandas df) - twitterAuthor ids and mention weights
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processMentionLabelWeights(self,mentionWeights):
        jsonData = list(mentionWeights.apply(lambda x: x.to_dict(), axis=1))
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.setMentionWeightLabelsBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e
            
    # update MENTION properties on TwitterAuthor nodes 
    # INPUTS:
    #    mentionStats (pandas df) - twitterAuthor id and mention/mentioned numbers
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def processMentions(self,mentionWeights):
        jsonData = list(mentionWeights.apply(lambda x: x.to_dict(), axis=1))
        print(jsonData[0])
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.setNMentionsBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e
            
# end of UserNodeClass.py