### TweetIngestClass.py
### Author: Andrew Larkin
### Date Created: July 20th, 2022
### Summary: Create and update properties of a node representing Twitter communities

class CommunityDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver=driver


    # code for creating community nodes and setting properties.  Does not create relationships to other nodes so it 
    # can run in parallel batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for creating community nodes and properties
    def setCommNodePropertiesApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MERGE (c:Community {id:label.community})
            SET c.nPosted = label.nPosted
            SET c.nChild = label.isChild
            SET c.nBaby = label.isBaby
            SET c.nToddler = label.isToddler
            SET c.nElem = label.isElem
            SET c.nMiddle = label.isMiddle
            SET c.nHigh = label.isHigh
            SET c.nPark = label.isPark
            SET c.nHome = label.isHome
            SET c.nSchool = label.isSchool
            SET c.nDaycare = label.isDaycare
            SET c.nNeighborhood = label.isNeighborhood
            SET c.nOutdoor = label.isOutdoor
            SET c.nIndoor = label.isIndoor
            SET c.nPhysical = label.isPhysical
            SET c.nEmotionalSocial = label.isEmotionalSocial
            SET c.nCognitive = label.isCognitive
            SET c.nPositive = label.isPositive
            SET c.nNegative = label.isNegative
            SET c.nUsers = label.tweetId",
        {batchSize:500,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code
    

    # code for creating community nodes and setting properties.  Does not create relationships to other nodes so it 
    # can run in parallel batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for creating community nodes and properties
    def setCommUserRelationships(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (c:Community {id:label.community})
        MATCH (t:TwitterUser {id:label.tweetId})
        WITH t,c
        MERGE (t)-[:BELONGS_TO]->(c)",
        {batchSize:500,iterateList:True,parallel:false,params:{labels:$labels}})
        """
        return code
    
    # insert batch of Community nodes.  Relationships between communities and other nodes are created
    # in different functions
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    commBatch (json array) - set of communities, each comm as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertCommBatch(self,tx,commBatch):
        cypher = self.setCommNodePropertiesApoc()
        try:
            result = tx.run(cypher,labels=commBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result of comm batch insertion")
            print(cypher)
            records = [record for record in result]
            print(records)
        return(result)
    
    # insert Comm nodes 
    # INPUTS:
    #    commBatch (pandas df) - comm id and associated properties.  One row for each comm
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def insertComm(self,commBatch):
        jsonData = list(commBatch.apply(lambda x: x.to_dict(), axis=1))
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.insertCommBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e
            
    # insert batch of Community nodes. Relationships between communities and other nodes are created
    # in different functions
    # INPUTS:
    #    tx (transaction) - open connection to Neo4j database
    #    commBatch (json array) - set of communities, each comm as an independent json object
    # OUTPUTS:
    #    result (str) - transaction result
    def insertCommRelationshipBatch(self,tx,commRelBatch):
        cypher = self.setCommUserRelationships()
        try:
            result = tx.run(cypher,labels=commRelBatch)
        except Exception as e:
            print(str(e))
        if(self.debug):
            print("this is the result of comm relationship batch insertion")
            print(cypher)
            records = [record for record in result]
            print(records)
        return(result)
    
    # insert Comm relationships
    # INPUTS:
    #    commBatch (pandas df) - comm id and associated properties.  One row for each comm
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def insertCommRelationships(self,commRelBatch):
        jsonData = list(commRelBatch.apply(lambda x: x.to_dict(), axis=1))
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.insertCommRelationshipBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e
    

