### TwitterAPI.py
### Author: Andrew Larkin
### Date Created: May 17, 2022
### Summary: Class for downloading tweets from the Twitter API and perform basic processing
### needed to store tweets in npy arrays.  Additional processing is performed by other scripts before
### data is ready for the target database

################ Import Libraries ##################

# For sending GET requests from the API
import requests
# For saving access tokens and for file management when creating and adding to the dataset
import os
# For dealing with json responses we receive from the API
import json
# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata
#To add wait time between requests
import time
import pandas as ps # for reading keywords from csv files
import numpy as np # for storing tweets
import base64 # for converting API secret to print
import hashlib
############ Define Class ############

class TwitterAPI:

    # initialize class
    # INPUTS:
    #    API_KEY (str) - Twitter API key - unique for each app
    #    SECRET (str) - Twitter secret - unique for each app
    #    imageFolder (str) - absolute filepath where images are stored
    #    tweetFolder (str) - absolute filepath where intermediate npy files are stored
    #    keywordFolder (str) - absolute filepath where csv files containing keywords are stored
    #    placeFolder (str) - absolute filepath where Twitter place id json data are stored
    def __init__(self,API_KEY,SECRET,imageFolder,tweetFolder,keywordFolder,placeFolder,userFolder):
        self.API_KEY = API_KEY
        self.SECRET = SECRET
        self.imageFolder = imageFolder
        self.tweetFolder = tweetFolder
        self.placeFolder = placeFolder
        self.userFolder = userFolder
        self.keywordDict = self.loadKeywords(keywordFolder)
        self.FIELDS_TO_DOWNLOAD = 'created_at,id,text,author_id,conversation_id,in_reply_to_user_id,referenced_tweets,geo'
        # note Twitter API is deprecated, no longer works
        #self.setupAccessToken()


    # setup the headers needed to verify an academic account request from the Twitter API
    def setupAccessToken(self):
        #Reformat the keys and encode them
        key_secret = '{}:{}'.format(self.API_KEY, self.SECRET).encode('ascii')
        #Transform from bytes to bytes that can be printed
        b64_encoded_key = base64.b64encode(key_secret)
        #Transform from bytes back into Unicode
        b64_encoded_key = b64_encoded_key.decode('ascii')

        base_url = 'https://api.twitter.com/'
        auth_url = '{}oauth2/token'.format(base_url)
        auth_headers = {
            'Authorization': 'Basic {}'.format(b64_encoded_key),
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
        }
        auth_data = {
            'grant_type': 'client_credentials'
        }
        auth_resp = requests.post(auth_url, headers=auth_headers, data=auth_data)
        print(auth_resp.status_code)
        print(auth_resp.json())
        access_token = auth_resp.json()['access_token']
        self.accessToken = access_token
        self.headers = {
            'Authorization': 'Bearer {}'.format(access_token)    
        }

    # load keywords used to search historical Twitter records 
    # INPUTS:
    #    keywordFolder (str) - absolute filepath where csv files containing keywords are stored
    # OUTPUTS:
    #    kwDict (dict) - contains keywords for multiple categories, one key for each categories
    def loadKeywords(self,keywordFolder):
        ageKeywords = list(ps.read_csv(keywordFolder + "age.csv")['keywords'])
        placeKeywords = list(ps.read_csv(keywordFolder + "place.csv")['keywords'])
        healthKeywords = list(ps.read_csv(keywordFolder + "health.csv")['keywords'])
        health2Keywords = list(ps.read_csv(keywordFolder + "health2.csv")['keywords'])
        kwDict = {
            'age':ageKeywords,
            'place':placeKeywords,
            'health':healthKeywords,
            'health2':health2Keywords
        }
        return(kwDict)

    # given a list of keywords, create a string of OR qualifications for a Twitter keyword serach
    # INPUTS:
    #    keywordList (str array) - list of keywords included in search criteria (use 'OR' operator)
    # OUTPUTS:
    #    httpString (str) - http code for the requested search 
    def convertKeywordsToHTTPCoding(self,keywordList):
        httpString = keywordList[0]
        for keyword in keywordList:
            if(keyword.find(' ') > -1):
                httpString += '%20OR%20' + '"' + keyword + '"'
            else:
                httpString += '%20OR%20' + keyword
        httpString += '%29'
        return(httpString)


    # serach twitter for a specific time range and list of keywords
    # INPUTS:
    #    keywords (str array) - list of keywords to serach tweets for
    #    fields (str array) - list of information that Twitter should return from the serach
    #    startTime (str) - timestamp in str format for the earliest tweet to return
    #    endTime (str) - timestamp in str format for the last tweet to return
    #    nextToken (str) - used if this is a continuation of a previusly requested search
    # OUTPUTS:
    #    response (request object) - contains results returned by the API request
    def queryTwitterAPI(self,keywords,fields,startTime,endTime,nextToken):
        prefix = "https://api.twitter.com/2/tweets/search/all"
        # restrict to original tweets (no retweets, US, and english)
        queryString = prefix +'?query=-is%3Aretweet%20lang%3Aen%20place_country%3AUS%20%28' + self.convertKeywordsToHTTPCoding(keywords) 
        # restrict to 500 results (max allowed) and information we want to collect about tweets
        queryString += '&max_results=500&tweet.fields=' + fields 
        # get media information including preview urls (for GIS and videos)
        queryString += '&expansions=attachments.media_keys,entities.mentions.username&media.fields=media_key,preview_image_url,url' + '&start_time='
        queryString += startTime + '&end_time=' + endTime
        # if current search is a continuation of a previously initiated search
        if(nextToken != ''):
            queryString += '&next_token=' + nextToken
        response = requests.get(queryString,headers=self.headers)
        return(response)
        

    # parse the data from a Twitter API request
    # INPUTS:
    #    response (request object) - contains results of the API request
    # OUTPUTS:
    #    data (json array) - list of tweet contents, one json element for each tweet
    #    media (json array) - list of media key contents, one json element for each media object
    #    nextToken (str) - used to continue collecting search if the search results are greating than the max results
    #                      that can be returned
    def processTweetQueryResponse(self,response):
        try:
            meta = response.json()['meta']
            # if now tweets were returned from the search
            if(response.json()['meta']['result_count']==0):
                return [[],[],'finished']
        # if no metadata was returned from the search (i.e. unsucssful search)
        except:
            return [[],[],'finished']
        data = response.json()['data']
        try:
            media = response.json()['includes']['media']
        # if none of the records in the returned results contain attached media
        except:
            media = []
        try:
            nextToken = response.json()['meta']['next_token']
        # if this is the final batch in the returned search results
        except:
            nextToken = "finished"
        return [data,media,nextToken]


    # parse the data from a Twitter API request
    # INPUTS:
    #    response (request object) - contains results of the API request
    # OUTPUTS:
    #    data (json array) - list of tweet contents, one json element for each tweet
    #    media (json array) - list of media key contents, one json element for each media object
    #    nextToken (str) - used to continue collecting search if the search results are greating than the max results
    #                      that can be returned
    def processUserQueryResponse(self,response):
        try:
            data = response.json()['data']
            return data
        except:
            return []

    # once tweets are parsed, save to disk
    # INPUTS:
    #    data (json array) - list of tweet contents, one json element for each tweet
    #    media (json array) - list of media key contents, one json element for each media object
    #    day (int) - day of month
    #    month (int) - month of year
    #    year (int) - year of tweets
    #    parentFolder (str) - absolute filepath where tweet info should be stored
    def saveDataToDisk(self,data,media,day,month,year,outFolder):
        dayStr = str(year).zfill(4) + str(month).zfill(2) + str(day).zfill(2)
        np.save(outFolder + "tw_" + dayStr + ".npy",np.array(data, dtype="object"))
        np.save(outFolder + "me_" + dayStr + ".npy",np.array(media, dtype="object"))

    # download, parse, and store one days worth of tweets which contain at least one of a set of keywords
    # INPUTS:
    #    day (int) - day of month
    #    month (int) - month of year
    #    year (int) - year of tweets
    #    inputKW (str array) - list of search keywords.  Tweets must contain at least 1 keyword
    #    fields (str array) - list of information to return from twitter serach
    #    parentFolder (str) - absolute filepath where tweets should be stored
    #    lastDay (bool) - whether the input variable 'day' is the last day of the month
    def processSingleTwitterDataOneDay(self,day,month,year,inputKW,lastDay,outFolder):
        data, media = self.queryTwitterOneDay(day,month,year,inputKW,lastDay)
        self.saveDataToDisk(data,media,day,month,year,outFolder)


    # download, parse, and store one year of tweets which contain at least one of a set of keywords
    # INPUTS:
    #    year (int) - year of interest
    #    inputKW (str array) - list of search keywords.  Tweets must contain at least 1 keyword
    #    fields (str array) - list of information to return from tweet search
    #    parentFolder (str) - absolute filepath where tweet information will be stored
    def processSingleTwitterOneYear(self,year,kwCat):
        DAYS_IN_MONTH = [31,28,31,30,31,30,31,31,30,31,30,31]
        # if a leap year, February has an extra day
        if(year%4 ==0):
            DAYS_IN_MONTH[1] = 29

        if(kwCat not in self.keywordDict.keys()):
            print("%s category not found in kewyord earch dictionary.  Options are: %s" %(kwCat,self.keywordDict.keys()))
            print("cannot download tweets")
            return
        
        searchList = self.keywordDict[kwCat]
        outFolder = self.tweetFolder + kwCat + "/"
        # for each month in the year
        for month in range(1,13):
            # for each day in the month, download and parse tweets and store in a unique file for this day
            for day in range(1,DAYS_IN_MONTH[month-1]+1):
                dayStr = str(year).zfill(4) + str(month).zfill(2) + str(day).zfill(2)
                outputData = outFolder + "tw_" + dayStr + ".npy"
                # if this is the last day in the month, the end date will be the first day of the following month
                if(day == DAYS_IN_MONTH[month-1]):
                    lastDay = True
                else:
                    lastDay = False
                # if tweets have already been downloaded for this day, skip the query and move on to the next day
                if not(os.path.exists(outputData)):
                    print("downloading tweets for %s" %(outputData))
                    self.processSingleTwitterDataOneDay(day,month,year,searchList,lastDay,outFolder)
                else:
                    print(outputData + " already exists")

    # given a place id, download place info and store on disk
    # INPUTS:
    #    placeId (str) - placeId for info to download
    def downloadPlaceJson(self,placeId):
        outputFilepath = self.placeFolder + placeId + ".json"
        urlPrefix = "https://api.twitter.com/1.1/geo/id/"
        urlSuffix = ".json"
        url = urlPrefix + str(placeId) + urlSuffix
        requestData = requests.get(url=url,headers=self.headers)
        placeJson = requestData.json()
        with open(outputFilepath, 'w',encoding='utf-8') as f:
            json.dump(placeJson,f,ensure_ascii=False,indent=4)

    # given a lit of place ids, download place infos and store on disk
    # INPUTS:
    #    placeIds (str list) - multiple placeIds for info to download
    def downloadPlaceSetJson(self,placeIds):
        for curId in placeIds:
            outputFilepath = self.placeFolder + curId + ".json"
            if(os.path.exists(outputFilepath)):
                print("%s already exists" %(outputFilepath))
            else:
                try:
                    self.downloadPlaceJson(curId)
                except Exception as e:
                    print("couldn't download json for place id: %s: %s" %(curId,str(e)))
                time.sleep(15)

    # given the image media key, use a hash function to determine what subfolder the image is stored in
    # INPUTS:
    #    mediaKey (str) - unique id for the media (image)
    #    nbins (int) - number of bins (i.e. has indexes)
    # OUTPUTS:
    #    name of subfolder the image is stored in
    def hashKey(self,mediaKey,nbins=10000):
        return str(abs(int(hashlib.sha512(mediaKey.encode('utf-8')).hexdigest(), 16))%nbins)

    # given a list of user ids, query Twitter for user info
    # INPUTS:
    #    userIds (str list) - list of user ids
    # OUTPUTS:
    #    json response to Twitter query
    def queryTwitterAPIUsers(self,userIds):
        queryString = "https://api.twitter.com/2/users?ids=" + userIds[0]
        for userId in userIds[1:]:
            queryString += "%2C" + userId
        queryString += '&user.fields=id,username,created_at,location'
        response = requests.get(queryString,headers=self.headers)
        return response

    # given user data response from Twitter, store on disk
    # INPUTS:
    #    userData (json) - contains user data
    #    batchId (int) - unique id for each user query
    def saveUserInfo(self,userData,batchId):
        hashFolder = self.userFolder + self.hashKey(batchId) + "/"
        if not(os.path.exists(hashFolder)): 
            os.mkdir(hashFolder)
        outputFile = hashFolder + batchId + ".npy"
        np.save(outputFile,np.array(userData, dtype="object"))

    # if user is not found, replace generic boilerplate in user node
    # INPUTS:
    #    error (str) - error returned from Twitter query
    def createNotFoundUser(self,error):
        return ({
            "id":error['value'],
            "location":"User Not Found",
            "created_at":"1900-01-01T00:00:00.000Z",
            "username":"User Not Found"
        })

    # for users not found, replace error code with user not found boilerplate
    # INPUTS:
    #    rawUserInfo (json) - user info in json format
    # OUTPUTS:
    #    errorUserInfo (json) - user info with error replaced by boilerplate
    def processUsersNotFound(self,rawUserInfo):
        errorUserInfo = []
        if('errors' in rawUserInfo.json().keys()):
            for error in rawUserInfo.json()['errors']:
                errorUserInfo.append(self.createNotFoundUser(error))
        return(errorUserInfo)

    # query Twitter for a batch of user ids and save to disk
    # INPUTS:
    #    userIds (str list) - list of user ids
    #    batchSize (int) - number of users to query with Twitter at a time
    def getTwitterUserInfo(self,userIds,batchSize=50):
        curIndex = 0
        batchData = []
        while(curIndex<len(userIds)):
            curUserBatch = userIds[curIndex:curIndex+batchSize]
            rawUserInfo = self.queryTwitterAPIUsers(curUserBatch)
            batchData += self.processUserQueryResponse(rawUserInfo)
            batchData += self.processUsersNotFound(rawUserInfo)
            time.sleep(15)
            curIndex+=batchSize
            print(curIndex)
        self.saveUserInfo(batchData,userIds[0])
            
    # get one days worth of tweets which contain at least one of a set of search keywords
    # INPUTS:
    #   day (int) - day of month
    #   month (int) - month of year
    #   year (int) - year of interest
    #   keywords (str array) - list of search keywords
    #   fields (str array) - list of tweet information to return
    #   lastDay (bool) - whether the input variable 'day' is the last day of the month
    # OUTPUTS:
    #   data (json array) - list of tweet contents, one json element for each tweet
    #   media (json array) - list of media key contents, one json element for each media object
    def queryTwitterOneDay(self,day,month,year,keywords,lastDay):
        startTime = str(year).zfill(4) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2) + 'T00:00:00Z'
        # if startTime is the last day of the month, then endTime will be the first day of the following month
        if(lastDay):
            if(month==12):
                endTime = str(year+1).zfill(4) + '-' + str(1).zfill(2) + '-' + str(1).zfill(2) + 'T00:00:00Z' 
            else:
                endTime = str(year).zfill(4) + '-' + str(month+1).zfill(2) + '-' + str(1).zfill(2) + 'T00:00:00Z'
        else:
            endTime = str(year).zfill(4) + '-' + str(month).zfill(2) + '-' + str(day+1).zfill(2) + 'T00:00:00Z'
        nextToken = ''
        data,media = [],[]
        index = 0
        # if the next token is not finished, there are still more tweets for this day to download and parse
        while(nextToken != "finished"):
            response = self.queryTwitterAPI(keywords,self.FIELDS_TO_DOWNLOAD,startTime,endTime,nextToken)
            tempData,tempMedia,nextToken = self.processTweetQueryResponse(response)
            data.append(tempData)
            media.append(tempMedia)

            # for debugging/ progress checking
            if(index%10==0):
                print(index)
            index+=1
            time.sleep(6) # must have at least 5 second wait to conform to Twitter API query limits
        return([data,media])

### END OF TwitterAPI.py ###