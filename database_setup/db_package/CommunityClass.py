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
        "MERGE (c:Community {id:label.comm_id})
            SET t.nPosted = label.nPosted
            SET t.nChild = label.isChild
            SET t.nBaby = label.isBaby
            SET t.nToddler = label.isToddler
            SET t.nElem = label.isElem,
            SET t.nMiddle = label.isMiddle,
            SET t.nHigh = label.isHigh,
            SET t.nPark = label.isPark,
            SET t.nHome = label.isHome,
            SET t.nSchool = label.isSchool,
            SET t.nDaycare = label.isDaycare,
            SET t.nNeighborhood = label.isNeighborhood,
            SET t.nOutdoor = label.isOutdoor,
            SET t.nIndoor = label.isIndoor,
            SET t.nPhysical = label.isPhysical,
            SET t.nEmotionalSocial = label.isEmotionalSocial,
            SET t.nCognitive = label.isCognitive,
            SET t.nPositive = label.isPositive,
            SET t.nNegative = label.isNegative
            SET t.nUsers = label.tweetId",
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
        "MATCH (c:Community {id:label.comm_id})
        MATCH (t:TwitterUser {id:label.user_id})
        WITH t,c
        MERGE (t)-[:BELONGS_TO]->(c)",
        {batchSize:500,iterateList:True,parallel:true,params:{labels:$labels}})
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
    
    # insert Comm nodes and relationship
    # INPUTS:
    #    commBatch (pandas df) - comm id and associated properties.  One row for each comm
    # OUTPUTS:
    #    results (str arrays) - results of the transactions performed to create the new tweet node and corresponding relationships
    def insertComm(self,commBatch):
        jsonData = list(commBatch.apply(lambda x: x.to_dict(), axis=1))
        print(jsonData[0])
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.insertCommBatch,jsonData)    
                print("completedBatch")
                return result
            except Exception as e:
                return e
            

    def setCommRelationships(self,commBatch):
        return 1
    

    

