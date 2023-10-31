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

    