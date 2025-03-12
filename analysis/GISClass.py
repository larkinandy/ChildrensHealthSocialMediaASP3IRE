############# GISClass.py ###########
### created by: Andrew Larkin
### Summary:
###    class for recognizing city and state names from social media records,
###    and for matching proper nouns in tweet text with OpenStreetMap

from fuzzywuzzy import process
import pandas as ps
import numpy as np
import json
import re
from multiprocessing import Pool
import os


class GIS:
    # initialize custom class with three geo files and location where output should be stored
    # INPUTS:
    #    geoFolder (str) - folderpath where GIS data for matching is stored
    #    outputFolder (str) - folderpath where analysis results should be stored
    def __init__(self,geoFolder,outputFolder):
        self.setupGeoFiles(geoFolder)
        self.outputfolder = outputFolder

    # load geo files into memory
    # INPUTS:
    #    geoFolder (str) - folderpath where GIS data for matching is stored
    #    debug (boolean) - whether or not debug statements should be printed to console
    def setupGeoFiles(self,geoFolder,debug=False):
        self.airportCodes = self.loadAirportCodes(geoFolder + "AirportCodesUS.csv")
        self.cityShort = self.loadCityShortnames(geoFolder + "cityShortNames.csv")
        self.cityNames = self.loadCityNames(geoFolder + "cityNames.csv")
        self.stateNames = self.loadStateNames(geoFolder + "cityNames.csv")
        if debug:
            print("completed setting up geo files")

    # load 3 letter airport codes into memory
    # INPUTS:
    #    inFilepath (str) - absolute filepath where airport codes are stored in csv format
    #    debug (boolean) - whether debug statements should be printed to console
    # OUTPUTS:
    #    airportCodes (str array) - list of airport codes
    def loadAirportCodes(self,inFilepath,debug=False):
        airportDF = ps.read_csv(inFilepath)
        airportCodes = list(set(airportDF['iata_code']))
        if debug:
            print("number of airport codes: %i" %(len(airportCodes)))
        return(airportCodes)
    
    # load city short names into memory
    # INPUTS:
    #    inFilepath (str) - absolute filepath where city names are stored in csv format
    # OUTPUTS:
    #    cityShort (str array) - list of city names
    def loadCityShortnames(self,inFilepath):
        cityShorttDF = ps.read_csv(inFilepath)
        cityShort = list(cityShorttDF['shortnames'])
        return(cityShort)
    
    # load state names into memory
    # INPUTS:
    #    inFilepath (str) - absolute filepath where state names are stored in csv format
    # OUTPUTS:
    #    cleanedList (str array) - list of state names
    def loadStateNames(self,inFilepath):
        cityData = ps.read_csv(inFilepath)
        stateNames = list(set(cityData['stname'].str.lower()))
        cleanedList = [x for x in stateNames if str(x) != 'nan']
        return(cleanedList)

    # load city aliases into memory
    # INPUTS:
    #    inFilepath (str) - absolute filepath where city aliases are stored in json format
    #    debug (boolean) - whether to print debug statements to console
    # OUTPUTS:
    #    aliases (str array) - list of city aliases
    def loadCityAliases(self,inFilepath,debug=False):
        aliases = []
        tempObj = open(inFilepath)
        aliasJson = json.load(tempObj)
        states = aliasJson.keys()
        for state in states:
            aliases += self.flattenAliases(aliasJson[state])
        tempObj.close()
        if debug:
            print("number of city aliases: %i" %(len(aliases)))
        return(aliases)
    
    # convert nested array into flat array
    # INPUTS:
    #    stateRecords (array of arrays) - nested array of cities within states
    # OUTPUTS:
    #    aliases (str array) - list of cities
    def flattenAliases(self,stateRecords):
        aliases = []
        citiesInState = stateRecords.keys()
        for city in citiesInState:
            cityAliases = stateRecords[city]
            for alias in cityAliases:
                aliases.append(alias)
        return(aliases)
    
    # load city full names from csv file
    # INPUTS:
    #    inFilepath (str) - absolute filepath where city names are stored
    #    debug (boolean) - whether to print debug statements to console
    # OUTPUTS:
    #    cityNames (str array) - list of full city names with city and state (e.g. Portland, Oregon)
    def loadCityNames(self,inFilepath,debug=False):
        cityData = ps.read_csv(inFilepath)
        cityData['nameFull'] = cityData['NAME'] + " " + cityData['stname']
        cityData['nameAbbr'] = cityData['NAME'] + cityData['abbr']
        cityNames = list(cityData['nameFull']) + list(cityData['nameAbbr'])
        if debug:
            print("number of city names: %i" %(len(cityNames)))
        return cityNames

    # attempt to match a self-reported user home town/city with one of four possible match types 
    # INPUTS:
    #    type (str) - type of attempted match. either cityFull' or 'airportCode'
    #    choices (str array) - acceptable matches
    #    location (str) - self-reported user home town/city
    # OUTPUTS:
    #    maxScore (int) - score for the highest quality match. (0 to 100, 100 is a perfect match)
    #    bestChoices (pandas df) - highest quality matches. The match in row 0 has the best score
    def createPredSingleType(self,type,choices,location):
        try:
            bestChoices = process.extract(location,choices,limit=1)
        except Exception as e:
            print(str(e))
        maxScore = bestChoices[0][1]
        try:
            bestChoices =  ps.DataFrame(bestChoices,columns=['bestChoice','closenessScore'])
        except Exception as e:
            print(str(e))
            bestChoices = ps.DataFrame({
                'bestChoice':['Error'],
                'closenessScore':[-1]
            })
        bestChoices['predType'] = type
        return(maxScore,bestChoices)

    # if a perfect match with a city name isn't possible, use this function to try a match with
    # either a shortened equivocal city name or state name
    # INPUtS:
    #    type (str) - type of attempted match. either 'cityShort' or 'state'
    #    choices (str array) - list of candidate matches
    #    location (str) - self-reproted user location
    # OUTPUTS:
    #    integer corresponding to match score. 100 indicates a match, 0 indicates no match 
    #    df (pandas dataframe) - highest quality matches. The match in row 0 has the best scored   
    def testWithinSingleType(self,type,choices,location):
        matches = list({x for x in choices if x in location.lower()})
        df = ps.DataFrame({'bestChoice':matches})
        if(df.count()[0] ==0):
            df['bestChoice'] = ['None']
        df['predType'] = type
        if(len(matches)>0):
            df['closenessScore'] = 100
            return([100,df])
        df['closenessScore'] = 0
        return([0,df])

    # attempt to match one self-reported user location with a city or, if not possible, state
    # INPUTS:
    #    data 
    #        index 0 - user id (str) - unique user id
    #        index 1 - userLocation (str) - self-reported user location
    # OUTPUTS:
    #    pandas datarame containing match details
    def processSingleUser(self,data):
            userId = data[0]
            userLocation = data[1]
            try:
                # remove extraneous details and symbols to increase likelihood of a perfect match
                sanitizedLocation = re.sub(' +', ' ',userLocation.replace(",", " ").replace("USA"," "))
                maxProb, curIndex = 0,0
                dataSources = [self.cityNames,self.airportCodes,self.cityShort,self.stateNames]
                sourceLabels = ['cityFull','airportCode','cityShort','state']
                while(maxProb<100 and curIndex<len(dataSources)):

                    # if matching on city full name or airport code, use the function createPredSingleType. Else, a perfect 
                    # match to an exact city isn't possible. Try to match to big cities or state name
                    if(curIndex < 2):
                        maxProb, df = self.createPredSingleType(sourceLabels[curIndex],dataSources[curIndex],sanitizedLocation)
                    else:
                        maxProb, df = self.testWithinSingleType(sourceLabels[curIndex],dataSources[curIndex],sanitizedLocation)

                    curIndex+=1
                df['id'] = str(userId) # user id
                df['loc'] = userLocation # self-reported user location
                df['sanitized'] = sanitizedLocation # 
                return(df)
            
            # return a pandas dataframe indicating a match wasn't found
            except Exception as e:
                return(
                ps.DataFrame({
                    'id':[str(userId)],
                    'bestChoice':['None'],
                    'closenessScore':[0],
                    'predType':['None'],
                    'loc': ['None']
                })
            )     
    
    # georeference a batch of self-reported user locations
    # INPUTS:
    #    inData (numpy array) - set of self-reported user locations and user ids 
    #    debug (boolean) - whether to print debug statements to console
    def georeferenceUserLocations(self,inData,debug=False):
        outputFilename = self.outputFolder + str(int(inData[0][0])) + ".csv"
        if(os.path.exists(outputFilename)):
            if debug:
                print("%s already exists" %(outputFilename))
            return
        dfArr = []
        for record in inData:
            dfArr.append(self.processSingleUser(record))
        df = ps.concat(dfArr)
        df.to_csv(outputFilename,index=False)

# end of GISClass.py