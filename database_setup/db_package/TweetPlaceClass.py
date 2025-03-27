### TweetPlaceClass.py
### Author: Andrew Larkin
### Date Created: May 19, 2022
### Summary: Class constructing and querying Tweet place nodes in a Neo4j database


class TweetPlaceDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver=driver

    # get ids that are missing place information
    # OUTPUTS:
    #    str list of place ids
    def getOrphanPlaceIds(self):
        def inLineFx(tx):
            query = """
                MATCH (p:TwitterPlace) WHERE p.name IS NULL
                RETURN p.id
            """
            result=tx.run(query)
            orphanPlaceIds = [row.values()[0] for row in result]
            return(orphanPlaceIds)

        with self.driver.session() as session:
            result = session.read_transaction(inLineFx)
            return result

    # code boilerplate for creating plae node
    # OUTPUTS:
    #    str code 
    def createCodeForPlaceInfo(self):
        code = """
            MATCH (p:TwitterPlace {id:$place_id})
            SET p.name = $short_name
            SET p.fullName = $full_name
            SET p.bboxLat = $bbox_lat
            SET p.booxLon = $bbox_lon
            SET p.centroid = $centroid
            SET p.placeType = $place_type
        """ 
        return code 
    
    # code for setting the city geo id property on a twitter place node
    # runs in batch mode
    # OUTPUTS:
    #     code (str) - code block used as part of a transaction for setting a relationship property
    def setCityGeoApoc(self):
        code = """
        CALL apoc.periodic.iterate('UNWIND $labels as label RETURN label',
        "MATCH (p:TwitterPlace {id:label.geo_id})
        SET p.cityGeo = label.geoid,
        p.cityName = label.name,
        p.extent = label.extent",
        {batchSize:50000,iterateList:True,parallel:true,params:{labels:$labels}})
        """
        return code

    # update place node with info
    # INPUTS:
    #    placeDict (dict) - dictionary containing info about one place
    # OUTPUTS:
    #    Neo4j transaction result
    def addPlaceInfo(self,placeDict):
        def inLineFxn(tx):
            query = self.createCodeForPlaceInfo()
            result = tx.run(
                query,
                place_id=placeDict['id'],
                short_name=placeDict['name'],
                full_name = placeDict['full_name'],
                bbox_lat = placeDict['bounding_box']['lat'],
                bbox_lon = placeDict['bounding_box']['lon'],
                centroid = placeDict['centroid'],
                place_type = placeDict['place_type']
            )
            return result
        with self.driver.session() as session:
            result = session.write_transaction(inLineFxn)
            return result
        
    # batch update place nodes with city attribute. 
    # INPUTS:
    #     placeBatch (json array) - list of nodes to update each json object corresponds to one
    #                              and only one twitterPlace node
    # OUTPUTS:
    #     result (array of custom neo4j objects) - result of the batch upsert transaction
    def setCityGeoBatch(self,tx,placeBatch):
        cypher = self.setCityGeoApoc()
        try:
            results = tx.run(cypher,labels=placeBatch)
            return (results)
        except Exception as e:
            print(str(e))
        return e


    def setCityGeo(self,geoInfo):
        jsonData = list(geoInfo.apply(lambda x: x.to_dict(),axis=1))
        with self.driver.session() as session:
            try:
                result = session.write_transaction(self.setCityGeoBatch,jsonData)
                print("completedBatch")
                return result
            except Exception as e:
                return e