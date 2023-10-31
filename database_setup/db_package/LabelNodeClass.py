import pandas as ps


class LabelDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver=driver

    def insertKeywordNode(self,tx,keyword,category,subCategory):
        code = """
        MERGE(k:Keyword {name:$keyword})
        With k
        MATCH(c: """ + str(category) + """ {type:$subCategory})
        MERGE (k)-[:IS_TYPE]->(c)
        RETURN k.name
        """

        result = tx.run(code,keyword=keyword,category=category,subCategory=subCategory)
        return(result)

    def loadKeywordsFromCSV(self,inputFilepath):
        keywordCSV = ps.read_csv(inputFilepath)
        print("number of keywords found: %i" %(keywordCSV.count()[0]))
        tuples = [tuple(x) for x in keywordCSV.to_numpy()]
        self.addKeywords(tuples)

    def addKeywords(self,keywordTuples):
        with self.driver.session() as session:
            for tupleVals in keywordTuples:
                session.write_transaction(self.insertKeywordNode,tupleVals[0],tupleVals[1],tupleVals[2])

    def addLabelCode(self,nodeType,labels):
        if(len(labels)==0):
            return("")
        code = """"""
        for index in range(len(labels)):
            curLabel = labels[index]
            curNode = str(nodeType[0]).lower() + str(index) + ""
            code += """
                with t
                MATCH(""" + curNode + """:""" + str(nodeType) + """{type:'""" + str(curLabel) + """'})
                MERGE (t)-[:DESCRIBES]->(""" + curNode + """)
            """
        return(code)

    def insertLabelRelationships(self,tx,cypher,twitter_id):
        result=tx.run(cypher,twitter_id=twitter_id).single()
        return(result)

    def addKeywordCode(self,keywords):
        if(len(keywords)==0):
            return
        code = """"""
        for index in range(len(keywords)):
            curKeyword = keywords[index]
            curNode = 'k' + str(index) + ""
            code += """
                with t
                MATCH(""" + curNode + """:Keyword""" + """{name:'""" + str(curKeyword) + """'})
                MERGE (t)-[:DESCRIBES]->(""" + curNode + """)
            """
        return(code)

    def insertKeywordRelationships(self,tx,cypher,twitter_id):
        result=tx.run(cypher,twitter_id=twitter_id).single()
        return(result)

    def addKeywordsToTweet(self,tweet_id,keywords):
        code = """
        MATCH (t:Tweet {id:$twitter_id})
        """
        code += self.addKeywordCode(keywords)
        code += "RETURN t.id"
        print(code)
        with self.driver.session() as session:
            result = session.write_transaction(self.insertKeywordRelationships,code,tweet_id)
            return result

    def addLabelsToTweet(self,tweet_id,healthLabels,placeLabels,ageLabels,directionLabels):
        code = """
        MATCH (t:Tweet {id:$twitter_id})
        """
        code += self.addLabelCode("Health",healthLabels)
        code += self.addLabelCode("Place",placeLabels)
        code += self.addLabelCode("Age",ageLabels)
        code += self.addLabelCode("HealthDirection",directionLabels)
        code += "RETURN t.id"

        with self.driver.session() as session:
            result = session.write_transaction(self.insertLabelRelationships,code,tweet_id)
            return result