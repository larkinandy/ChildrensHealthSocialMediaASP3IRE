# GeoNodeClass.py 
# Author: Andrew Larkin
# Summary: Perform operations on the Geo nodes
# Part of a set of classes for the ASP3IRE project

from unittest import result
import numpy as np
import secrets
import pandas as ps

class GeoDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, driver):
        self.driver=driver
        self.FIPSMap = self.createFIPSMap(secrets.StateFPCodes)

    # given a csv containing states, create state nodes
    # INPUTS:
    #    statesFilepath (str) - absolute filepath to csv containing state 
    #    couuntry (str) - country name
    # OUTPUTS:
    #    Neo4j transaction result
    def insertStatesFromCSV(self,statesFilepath,country):
        
        rawData = ps.read_csv(statesFilepath)
        stateNames = list(rawData['Name'])
        def inLinFxn(tx,state,country):
            query = """
            MATCH(c:Country {name:$country})
            MERGE(s:State {name:$state})
            with c,s
            MERGE(s)-[:IS_IN]->(c)
            """
            result = tx.run(query,state=state,country=country)
            return result

        with self.driver.session() as session:
            for state in stateNames:
                result = session.write_transaction(inLinFxn,state,country)
            return result

    # given csv of FIPS codes, create a map between fips codes and names
    #    fipsFilepath (str) - absolute filepath to csv containing FIPS 
    # OUTPUTS:
    #    map between name and FIPS
    def createFIPSMap(self,fipsFilepath):
        rawData = ps.read_csv(fipsFilepath)
        names = list(rawData['Name'])
        FIPS = list(rawData['FIPS'])
        FIPSMap = {}
        for index in range(len(rawData)):
            FIPSMap[FIPS[index]]=names[index]
        return(FIPSMap)

    # get list of cities
    # OUTPUTS:
    #    list of cities with id, name, and state
    def getCities(self):
        def inLineFxn(tx):
            query = """
            MATCH(c:City)
            return c.id, c.name, c.state
            """
            result=tx.run(query)
            cityStats = [row.values()[0:3] for row in result]
            return(cityStats)

        with self.driver.session() as session:
            result = session.read_transaction(inLineFxn)
            return result

    # load cities from csv and create city nodes
    # INPUTS:
    #    inputFilepath (str) - absolute filepath to csv file containing city info
    def loadCitiesFromCSV(self,inputFilepath):
        citiesCSV = ps.read_csv(inputFilepath)
        print("number of cities found: %i" %(citiesCSV.count()[0]))
        tuples = [tuple(x) for x in citiesCSV.to_numpy()] 
        self.insertCities(tuples)

    # create city nodes including child stats
    # INPUTS:
    #    city_tuples (tuple list) - one tuple for each city
    # OUTPUTS:
    #    Neo4j transaction result     
    def insertCities(self,city_tuples):
        def inLineFxn(tx,city_tuple):
            query = """
            MERGE(c:City {id:$city_id,name:$city_name,state:$state,c0_4_14:$c0_14,c5_9_14:$c5_14,c10_14_14:$c14_14,c15_17_14:$c17_14,c0_17_14:$c_all,
            c0_4_19:$c0_19,c5_9_19:$c5_19,c10_14_19:$c10_19,c15_17_19:$c15_19,c0_17_19:$c_all_19
            })
            with c
            MATCH (s:State {name:$state})
            MERGE (c)-[:IS_IN]->(s)
            return c.id, c.name, c.state
            """
            result=tx.run(
                query,city_id=str(city_tuple[13]),city_name=city_tuple[12],state=self.FIPSMap[int(city_tuple[11])],
                c0_14=int(city_tuple[1]),c5_14=int(city_tuple[2]),c14_14=int(city_tuple[3]),c17_14=int(city_tuple[4]),c_all=int(city_tuple[5]),
                c0_19=int(city_tuple[6]),c5_19=int(city_tuple[7]),c10_19=int(city_tuple[8]),c15_19=int(city_tuple[9]),c_all_19=int(city_tuple[10])
            )
            return(result)

        with self.driver.session() as session:
            for city_tuple in city_tuples:
                result = session.write_transaction(inLineFxn,city_tuple)
        return result

    # get information from a city node
    # INPUTS:
    #    city_id (str) - unique id for city
    # OUTPUTS:
    #    dictionary containing city info
    def getCityStats(self,city_id):
        def inLineFxn(tx,city_id):
            query = """
            MATCH(c:City {id:$city_id})
            RETURN c
            """
            result = tx.run(
                query,city_id=city_id
            ).single()
            return result

        with self.driver.session() as session:
            result = session.read_transaction(inLineFxn,city_id)
        return result

    # load school districts from csv and create nodes 
    # INPUTS:
    #    inputFilepath (str) - absolute filepath to csv file
    def loadDistrictsFromCSV(self,inputFilepath):
        districtsCSV = ps.read_csv(inputFilepath)
        print("number of districts found: %i" %(districtsCSV.count()[0]))
        tuples = [tuple(x) for x in districtsCSV.to_numpy()] 
        self.insertDistricts(tuples)

    # create school district nodes from district tuples
    # INPUTS:
    #    district_tuples (array of tuples) - one tuple for each district
    # OUTPUTS:
    #    Neo4j transaction result
    def insertDistricts(self,district_tuples):
        def inLineFxn(tx,district_tuple):
            query = """
            MERGE(d:District {id:$district_id,name:$district_name,state:$state,c_2020:$c2020,c_2015:$c2015,c_2010:$c2010})
            with d
            MATCH (s:State {name:$state})
            MERGE (d)-[:IS_IN]->(s)
            return d.id, d.name, d.state
            """
            result=tx.run(
                query,district_id=str(district_tuple[2]),district_name=district_tuple[3],state=self.FIPSMap[int(district_tuple[1])],
                c2020=district_tuple[4],c2015=district_tuple[5],c2010=district_tuple[6]
            )
            return(result)

        with self.driver.session() as session:
            for city_tuple in district_tuples:
                result = session.write_transaction(inLineFxn,city_tuple)
        return result

    # get information from a school district node
    # INPUTS:
    #    district_id (str) - unique id for district
    # OUTPUTS:
    #    Neo4j transaction result
    def getDistrictInfo(self,district_id):
        def inLineFxn(tx,district_id):
            query = """
            MATCH(d:District {id:$district_id})
            RETURN d
            """
            result = tx.run(
                query,district_id=district_id
            ).single()
            return result

        with self.driver.session() as session:
            result = session.read_transaction(inLineFxn,district_id)
        return result

    # load census tracts from csv and create census tract nodes
    # INPUTS:
    #    inputFilepath (str) - absolute filepath to census tract csv
    def loadCensusTractsFromCSV(self,inputFilepath):
        tractCSV = ps.read_csv(inputFilepath)
        print("number of tracts found: %i" %(tractCSV.count()[0]))
        tuples = [tuple(x) for x in tractCSV.to_numpy()] 
        self.insertTracts(tuples)

    # create census tracts
    # INPUTS:
    #    tract_tuples (list of tuples) - each tuple contains data for one census tract
    # OUTPUTS:
    #    Neo4j transaction result
    def insertTracts(self,tract_tuples):
            def inLineFxn(tx,tract_tuple):
                query = """
                MERGE(t:CensusTract {id:$tract_id,state:$state,c0_4_14:$c0_14,c5_9_14:$c5_14,c10_14_14:$c14_14,c15_17_14:$c17_14,c0_17_14:$c_all,
                c0_4_19:$c0_19,c5_9_19:$c5_19,c10_14_19:$c10_19,c15_17_19:$c15_19,c0_17_19:$c_all_19
                })
                with t
                MATCH (s:State {name:$state})
                MERGE (t)-[:IS_IN]->(s)
                return t.id, t.state
                """
                result=tx.run(
                    query,tract_id=str(tract_tuple[2]),state=self.FIPSMap[int(tract_tuple[1])],
                    c0_14=int(tract_tuple[8]),c5_14=int(tract_tuple[9]),c14_14=int(tract_tuple[10]),c17_14=int(tract_tuple[11]),c_all=int(tract_tuple[12]),
                    c0_19=int(tract_tuple[3]),c5_19=int(tract_tuple[4]),c10_19=int(tract_tuple[5]),c15_19=int(tract_tuple[6]),c_all_19=int(tract_tuple[7])
                )
                return(result)

            with self.driver.session() as session:
                for tract_tuple in tract_tuples:
                    result = session.write_transaction(inLineFxn,tract_tuple)
            return result
    
    # get information from a census tract node
    # INPUTS:
    #    tract_id (str) - census tract id
    # OUTPUTS:
    #    dict of census tract info
    def getTractInfo(self,tract_id):
        def inLineFxn(tx,tract_id):
            query = """
            MATCH(c:CensusTract {id:$tract_id})
            RETURN c
            """
            result = tx.run(
                query,tract_id=tract_id
            ).single()
            return result

        with self.driver.session() as session:
            result = session.read_transaction(inLineFxn,tract_id)
        return result

    # load OSM objects from csv file and create nodes
    # INPUTS:
    #    inputFilepath (str) - absolute filepath to OSM csv file
    def loadOSMObjectsFromCSV(self,inputFilepath):
        objectCSV = ps.read_csv(inputFilepath)
        print("number of objects found: %i" %(objectCSV.count()[0]))
        tuples = [tuple(x) for x in objectCSV.to_numpy()]
        self.insertOSMPlaces(tuples)

    # create nodes representing OSM locations
    # INPUTS:
    #    osmTuples (list of tuples) - each tuple represents one OSM location
    # OUTPUTS:
    #    Neo4j transaction result
    def insertOSMPlaces(self,osmTuples):
        def inLineFxn(tx,obj_tuple):
            query = """
            MERGE(p:OSMPlace {id:$obj_id, name:$obj_name})
            with p
            MATCH(t:PlaceType {type: $obj_type})
            MERGE (p)-[:IS_TYPE]->(t)
            return t.id
            """
            result = tx.run(
                query,obj_id = obj_tuple[2],obj_name = obj_tuple[0],obj_type=obj_tuple[1]
            ).single()
            return(result)

        with self.driver.session() as session:
            for obj_tuple in osmTuples:
                result = session.write_transaction(inLineFxn,obj_tuple)
        return(result)

    # create relationships between OSM and census tract nodes
    # INPUTS:
    #    relationshipTuples (list of tuples) - each tuple represents one relationship
    # OUTPUTS:
    #    Neo4j transaction result
    def insertObjectTractRelationships(self,relationTuples):
        def inLineFxn(tx,relationTuple):
            query = """
            MATCH (o:OSMPlace {id:$obj_id})
            MATCH (t:CensusTract {id:$ct_id})
            with o,t
            MERGE (o)-[:IS_IN]->(t)
            return o
            """
            try:
                result = tx.run(
                    query,obj_id=int(relationTuple[0]),ct_id=str(int(relationTuple[1]))
                ).single()
                return result
            except Exception as e:
                print(str(e))
                print(relationTuple)
            return None

        with self.driver.session() as session:
            for tupleVal in relationTuples:
                result = session.write_transaction(inLineFxn,tupleVal)
        return result

    # load relationships from csv file and create relationships between OSM and census tract nodes
    # INPUTS:
    #    inputFilepath (str) - absolute filepath to relationships
    # OUTPUTS:
    #    Neo4j transaction results
    def loadObjectTractRelationshipsFromCSV(self,inputFilepath):
        objectCSV = ps.read_csv(inputFilepath)
        print("number of relationships found: %i" %(objectCSV.count()[0]))
        objectCSV = objectCSV[['UNIQUE_ID','GEOID']]
        tuples = [tuple(x) for x in objectCSV.to_numpy()]
        result = self.insertObjectTractRelationships(tuples)
        print(result)

    # create relationships between OSM locations and school districts
    # INPUTS:
    #    relationshipTuples (list of tuples) - each tuple represents one relationship
    # OUTPUTS:
    #    Neo4j transaction result
    def insertObjectDistrictRelationships(self,relationTuples):
        def inLineFxn(tx,relationTuple):
            query = """
            MATCH (o:OSMPlace {id:$obj_id})
            MATCH (d:District {id:$district_id})
            with o,d
            MERGE (o)-[:IS_IN]->(d)
            return o
            """
            try:
                result = tx.run(
                    query,obj_id=int(relationTuple[0]),district_id=str(int(relationTuple[1]))
                ).single()
                return result
            except Exception as e:
                print(str(e))
                print(relationTuple)
            return None

        with self.driver.session() as session:
            for tupleVal in relationTuples:
                result = session.write_transaction(inLineFxn,tupleVal)
        return result

    # load relationships between school districts and OSM locations from csv and 
    # create relationships
    # INPUTS:
    #    inputFilepath (str) - absolute filepath to relationship csv file
    # OUTPUTS:
    #    Neo4j transaction result
    def loadObjectDistrictRelationshipsFromCSV(self,inputFilepath):
        objectCSV = ps.read_csv(inputFilepath)
        print("number of relationships found: %i" %(objectCSV.count()[0]))
        objectCSV = objectCSV[['UNIQUE_ID','GEOID']]
        tuples = [tuple(x) for x in objectCSV.to_numpy()]
        result = self.insertObjectDistrictRelationships(tuples)
        return(result)

    # create relationships between OSM locations and city nodes
    # INPUTS:
    #    rawData (array of dict) - one dict for each city-OSM relationship
    # OUTPUTS:
    #    Neo4j transaction result
    def insertObjectCityRelationships(self,rawData):
        def inLineFxn(tx,cityId,objIds):
            query="""
            MATCH (c:City {id:$cityId})
            with c
            UNWIND $objIds as objid
            MATCH (o:OSMPlace {id:objid})
            with o,c
            MERGE (o)-[:IS_NEAR]->(c)
            return o.id
            """
            try:
                print('oy')
                result = tx.run(
                    query,cityId =cityId,objIds=objIds
                ).single()
                return result
            except Exception as e:
                print(str(e))
            return None

        with self.driver.session() as session:
            for cityRow in rawData:
                print('a')
                result = session.write_transaction(inLineFxn,cityRow['cityid'],cityRow['objid'])
        return result

    # load relationships between OSM locations and cities from numpy data
    # INPUTS:
    #    inputFilepath (str) - absolute filepath to .npy file
    # OUTPUTS:
    #    list of dictionaries, one dictionary for each relationship
    def loadObjectCityRelationshipsFromNPY(self,inputFilepath):
        rawData = np.load(inputFilepath,allow_pickle=True)
        rawData = list(rawData)
        result = self.insertObjectCityRelationships(rawData[2:])
        return(result)
    