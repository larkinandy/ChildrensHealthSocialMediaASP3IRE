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
        


