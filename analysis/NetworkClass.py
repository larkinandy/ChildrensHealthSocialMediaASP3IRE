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
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """
    def __init__(self, queryFolder,loadWeights = False,loadGraph = True,debug=False):
        self.queryFolder=queryFolder
        self.childAttr = ['Child','Baby','Toddler','Elem','Middle','High']
        self.placeAttr = ['Park','Home','School','Neighborhood','Daycare']
        self.healthAttr = ['Cognitive','EmotionalSocial','Physical']
        self.allAttr = self.childAttr + self.placeAttr + self.healthAttr
        self.weightFilename = 'edgeWeights.csv'
        self.communityFilename = 'communities.csv'
        self.debug=debug
        if(loadWeights):
            self.loadWeights()
        if(loadGraph):
            self.createNetworkFromEdges()
        self.loadCounts()

    def calcEdgeWeights(self,debug):
        chunksize = 1000000
        index = 0
        updatedEdges = []
        for edges in ps.read_csv(self.queryFolder + "mention_weights.csv", chunksize=chunksize):
            print("processing mentions batch %i" %(index))
            if(debug and index > 2):
                df = ps.concat(updatedEdges)
                df.to_csv(self.queryFolder + "edgeWeights.csv",index=False)
                return
            edges = edges.rename(columns={'u.id':'sourceNodeId','b.id':'targetNodeId','m.n':'count'})
            edges = edges[~(edges['sourceNodeId'] == edges['targetNodeId'])]
            #print("number of edges in dataset: %i" %(edges.count))
            index+=1
            updatedEdges.append(self.calcSimilarityScores(edges))
        df = ps.concat(updatedEdges)
        df.to_csv(self.queryFolder + "edgeWeights.csv",index=False)

    def createNetworkFromEdges(self):
        if(self.debug): 
            edgesWithWeights = ps.read_csv(self.queryFolder + self.weightFilename,nrows=1000)
        else:
            edgesWithWeights = ps.read_csv(self.queryFolder + self.weightFilename)
        G = nx.convert_matrix.from_pandas_edgelist(edgesWithWeights,'sourceNodeId','targetNodeId','weight')
        self.G = nxcg_G = nxcg.from_networkx(G)
        print("graph G has %i edges" %(edgesWithWeights.count().iloc[0]))


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

    def identifyCommunities(self,graphRes=1.0):
        commSets = nx.community.louvain_communities(self.G, resolution=graphRes)
        df = self.convertCommToDF(commSets)
        df.to_csv(self.queryFolder + self.communityFilename,index=False)

    def loadWeights(self):
        if(self.debug):
            self.weights = ps.read_csv(self.weightFilename,nrows=1000)
        else:
            self.weights = ps.read_csv(self.weightFilename)

    def calcSimilarityScores(self,edgeData):
        count = 0
        sourceMerge = edgeData.merge(self.nodeData,left_on=['sourceNodeId'],right_on=['nodeId'])
        targetMerge = sourceMerge.merge(self.nodeData,left_on=['targetNodeId'],right_on=['nodeId'])
        sourceVectors = targetMerge[['percent' + val + '_x' for val in self.allAttr]]
        targetVectors = targetMerge[['percent' + val + '_y' for val in self.allAttr]]
        dotProd = (sourceVectors.values * targetVectors.values).sum(axis=1)
        norm1 = sourceVectors.apply(norm,axis=1)
        norm2 = targetVectors.apply(norm,axis=1)
        similarity = dotProd/np.maximum(norm1*norm2,0.000001)
        updatedEdge = targetMerge[['sourceNodeId','targetNodeId','count']]
        updatedEdge['similarity'] = similarity
        updatedEdge['weight'] = updatedEdge['similarity']*updatedEdge['count']
        updatedEdge = updatedEdge[updatedEdge['weight']>0]
        return(updatedEdge)

    def loadMentionWeights(self):
        if(self.debug):
            mentions = ps.read_csv(self.queryFolder + "mention_weights.csv",nrows=1000)
        else:
            mentions = ps.read_csv(self.queryFolder + "mention_weights.csv")
        mentions = mentions.rename(columns={'u.id':'sourceNodeId','b.id':'targetNodeId','m.n':'count'})
        return mentions

    def loadCountsOneFile(self,inFile,cats):
        rawData = ps.read_csv(inFile)
        rawData = rawData.rename(columns={"tweetId":"nodeId"})
        catsToKeep = ['nodeId']
        for cat in cats:
            rawData['percent' + cat] = rawData['is' + cat]/rawData['nPosted']*100
            catsToKeep.append('percent' + cat)
        rawData = rawData[catsToKeep]
        return(rawData)

    def loadCounts(self):
        childCounts = self.loadCountsOneFile(self.queryFolder + "childCounts.csv",self.childAttr)
        placeCounts = self.loadCountsOneFile(self.queryFolder + "placeCounts.csv",self.placeAttr)
        healthCounts = self.loadCountsOneFile(self.queryFolder + "healthCounts.csv",self.healthAttr)
        mergedCounts = childCounts.merge(placeCounts,how='outer',on='nodeId').merge(healthCounts,how='outer',on='nodeId')
        mergedCounts.fillna(0)
        self.nodeData = mergedCounts
        return(mergedCounts.count().iloc[0])

