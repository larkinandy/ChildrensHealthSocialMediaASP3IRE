### ConversationNodeClass.py
### Author: Andrew Larkin
### Date Created: May 18, 2022
### Summary: Perform ops on Neo4j conversation nodes



class ConversationDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver=driver


    # select users who have a relationship with the (:Analyzed {type:'orphan'}) node.  These are
    # users who have been identified as invovled in a tweet, but whose meta information has not yet
    # been collected
    def selectStaleConversations(self):
        def inLineFxn(tx):
            query = """
                MATCH (c:Conversation)-[:IN_STAGE]-(:Analyzed {type:'stale'})
                RETURN c.id
            """
            result=tx.run(query)
            userIds = [row.values()[0] for row in result]
            return(userIds)

        with self.driver.session() as session:
            result = session.read_transaction(inLineFxn)
            return result