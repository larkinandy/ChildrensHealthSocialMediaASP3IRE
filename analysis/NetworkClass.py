### NetworkClass.py
### Author: Andrew Larkin
### Date Created: July 20th, 2024
### Summary: Class for constructing and analyzing networks

import pandas as ps
import numpy as np
from numpy.linalg import norm
import networkx as nx
import nx_cugraph as nxcg

class Network:

    # create an instance of the Network class
    # INPUTS:
    #    queryFolder (str) - absolute filepath where network data is stored
    #    loadWeights (boolean) - whether weights need to calculated
    #    loadGraph (boolean) - whether a graph needs to be calculated
    #    debug (boolean) - whether to load subsets of data and print output statements
    def __init__(self, queryFolder,loadWeights = False,loadGraph = True,debug=False):
        self.queryFolder=queryFolder
        self.childAttr = ['Child','Baby','Toddler','Elem','Middle','High'] # child variables
        self.placeAttr = ['Park','Home','School','Neighborhood','Daycare'] # safe place variables
        self.healthAttr = ['Cognitive','EmotionalSocial','Physical'] # health variables
        self.allAttr = self.childAttr + self.placeAttr + self.healthAttr
        self.weightFilename = 'edgeWeights.csv'
        self.communityFilename = 'communities.csv'
        self.debug=debug

        # load edge weights from csv
        if(loadWeights):
            self.loadWeights()

        # create network x graph from edge weights
        if(loadGraph):
            self.createNetworkFromEdges()

        # load number of child, safe place, and health posts for each user 
        self.loadCounts()

    # calculate edge weights based on similarity of content of user posts
    def calcEdgeWeights(self,debug):
        chunksize = 1000000 # number of records to load into memory at once
        index = 0
        updatedEdges = [] # list where updated edge scores will be stored
        for edges in ps.read_csv(self.queryFolder + "mention_weights.csv", chunksize=chunksize):
            print("processing mentions batch %i" %(index))
            if(debug and index > 2):
                df = ps.concat(updatedEdges)
                df.to_csv(self.queryFolder + "edgeWeights.csv",index=False)
                return
            edges = edges.rename(columns={'u.id':'sourceNodeId','b.id':'targetNodeId','m.n':'count'})
            edges = edges[~(edges['sourceNodeId'] == edges['targetNodeId'])]
            index+=1
            updatedEdges.append(self.calcSimilarityScores(edges))

        # save updated edge weights to csv
        df = ps.concat(updatedEdges)
        df.to_csv(self.queryFolder + "edgeWeights.csv",index=False)

    # given a set of edges (including source and target node ids) create a cugraph
    def createNetworkFromEdges(self):

        # load edge weights into memory
        if(self.debug): 
            edgesWithWeights = ps.read_csv(self.queryFolder + self.weightFilename,nrows=1000)
        else:
            edgesWithWeights = ps.read_csv(self.queryFolder + self.weightFilename)

        # create networkx gratph
        G = nx.convert_matrix.from_pandas_edgelist(edgesWithWeights,'sourceNodeId','targetNodeId','weight')

        # convert networkx graph to a cugraph
        self.G = nxcg_G = nxcg.from_networkx(G)
        print("graph G has %i edges" %(edgesWithWeights.count().iloc[0]))

    # convert list of user ids for each community to a DF
    # INPUTS:
    #    comm (list of lists) - each sublist contains a list of user ids that belong to the community
    # OUTPUTS:
    #    df (pandas df) - dataframe where each row contains a community:user id pair
    def convertCommToDF(self,comm):
        commNum = 0
        commList,commNums = [],[]
        for com in comm:
            for member in com:
                commList.append(member)
                commNums.append(commNum)
            commNum+=1
        df = ps.DataFrame({
            'tweetId':commList,
            'community':commNums
        })
        return df

    # given a cugraph with edge weights, identify communities using the louvian algorithm
    def identifyCommunities(self,graphRes=1.0):

        # identify communities.  Results are returned as a list
        # each list contains node ids that belong to the community
        commSets = nx.community.louvain_communities(self.G, resolution=graphRes)

        # convert list to data frame and store as a .csv
        df = self.convertCommToDF(commSets)
        df.to_csv(self.queryFolder + self.communityFilename,index=False)

    # load edge weights from csv
    def loadWeights(self):
        if(self.debug):
            self.weights = ps.read_csv(self.weightFilename,nrows=1000)
        else:
            self.weights = ps.read_csv(self.weightFilename)

    # calculate the cosine similarity scores betweeen two user profiles
    # INPUTS:
    #    edgeData (pandas dataframe) - contains edge weights
    # OUTPUTS:
    #    updatedEdge pandas dataframe) - input dataset, updated with similarity and new edge weights
    def calcSimilarityScores(self,edgeData):

        # merge edge weights with user profiles for the source and target user ids
        sourceMerge = edgeData.merge(self.nodeData,left_on=['sourceNodeId'],right_on=['nodeId'])
        targetMerge = sourceMerge.merge(self.nodeData,left_on=['targetNodeId'],right_on=['nodeId'])
        sourceVectors = targetMerge[['percent' + val + '_x' for val in self.allAttr]]
        targetVectors = targetMerge[['percent' + val + '_y' for val in self.allAttr]]

        # calculate the cosine similarity based on differenes in user profiles
        dotProd = (sourceVectors.values * targetVectors.values).sum(axis=1)
        norm1 = sourceVectors.apply(norm,axis=1)
        norm2 = targetVectors.apply(norm,axis=1)
        similarity = dotProd/np.maximum(norm1*norm2,0.000001)

        # multiply similarity by original weight to get new edge weight
        updatedEdge = targetMerge[['sourceNodeId','targetNodeId','count']]
        updatedEdge['similarity'] = similarity
        updatedEdge['weight'] = updatedEdge['similarity']*updatedEdge['count']
        updatedEdge = updatedEdge[updatedEdge['weight']>0]
        return(updatedEdge)

    # load edge weights into memory and update column names
    # OUTPUTS:
    #    mentions (pandas dataframe) - contains edge weights
    def loadMentionWeights(self):
        if(self.debug):
            mentions = ps.read_csv(self.queryFolder + "mention_weights.csv",nrows=1000)
        else:
            mentions = ps.read_csv(self.queryFolder + "mention_weights.csv")
        mentions = mentions.rename(columns={'u.id':'sourceNodeId','b.id':'targetNodeId','m.n':'count'})
        return mentions

    # load user profile for one category (child, place, or health)
    # INPUTS:
    #    inFile (str) - absolute filepath where user profile is stored
    #    cats (str array) - column names to calculate percents for
    # OUTPUTS:
    #    rawData (pandas dataframe) - user profile for calculating similarity scores
    def loadCountsOneFile(self,inFile,cats):
        rawData = ps.read_csv(inFile) # road into memory
        rawData = rawData.rename(columns={"tweetId":"nodeId"}) 
        catsToKeep = ['nodeId']
        # calculate percent for categories of interest
        for cat in cats:
            rawData['percent' + cat] = rawData['is' + cat]/rawData['nPosted']*100
            catsToKeep.append('percent' + cat)
        rawData = rawData[catsToKeep]
        return(rawData)

    # load user profile from csv files 
    # OUTPUTS:
    #    mergedCounts (pandas dataframe) - user profiles
    def loadCounts(self):
        childCounts = self.loadCountsOneFile(self.queryFolder + "childCounts.csv",self.childAttr) # user profile for child content
        placeCounts = self.loadCountsOneFile(self.queryFolder + "placeCounts.csv",self.placeAttr) # user profile for safe places content 
        healthCounts = self.loadCountsOneFile(self.queryFolder + "healthCounts.csv",self.healthAttr) # user profile for health content
        mergedCounts = childCounts.merge(placeCounts,how='outer',on='nodeId').merge(healthCounts,how='outer',on='nodeId')
        mergedCounts.fillna(0)
        self.nodeData = mergedCounts
        return(mergedCounts.count().iloc[0])
    
    # for each category of interest, count the number of tweets posted by each community and store results in csv
    def countTweetsPerCommmunity(self):

        # load community:userid pairs
        communityAssignments = ps.read_csv(self.queryFolder + "communities.csv")
        childData = ps.read_csv(self.queryFolder + "childCounts.csv") # load number of child tweets from each user
        placeData = ps.read_csv(self.queryFolder + "placeCounts.csv") # load number of place tweets from each user
        healthData = ps.read_csv(self.queryFolder + "healthCounts.csv") # load number of health tweets from each user

        # merge tweet profiles from each category and count for each community
        mergedData = childData.merge(placeData, how='outer',on='tweetId').merge(healthData, how='outer',on='tweetId')
        mergedData = mergedData.merge(communityAssignments,how='inner',on='tweetId').fillna(0)
        commMetrics = mergedData.groupby('community').sum()
        commMetrics.reset_index(inplace=True)
        commMetrics = commMetrics[['community','nPosted','isChild','isBaby','isToddler','isElem','isMiddle','isHigh','isHome','isSchool','isDaycare','isPark','isNeighborhood','isOutdoor','isIndoor','isEmotionalSocial','isCognitive','isPhysical','isPositive','isNegative']].astype(int)

        # count number of users in each community and add to community profile dataset
        nUsers = mergedData.groupby('community').count()
        nUsers.reset_index(inplace=True)
        nUsers = nUsers[['community','tweetId']]
        commMetrics = commMetrics.merge(nUsers,how='inner',on='community')
        commMetrics.to_csv(self.queryFolder + "communityMetrics.csv",index=False)

# end of NewtorkClass.py