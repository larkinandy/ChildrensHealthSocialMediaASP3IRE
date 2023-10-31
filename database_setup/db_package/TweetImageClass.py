
### TimeImageClass.py
### Author: Andrew Larkin
### Date Created: May 18, 2022
### Summary: Class for downloading, storing, and retreiving images and corresponding metadata from 
### tweets

################ Import Libraries ##################

import requests # to get image from the web
import shutil # to save it locally
import numpy as np # for loading image meta from np files
import os # for searching on disk to see if file already exists
import glob # to locate all image meta files stored in a folder
import hashlib # hash is a part of the filepath for storing images in manageable-sized subsets
import shutil


class TweetImage:

    # initialize instance of class064908
    # INPUTS:
    #    imageFolder (str) - absolute filepath where images are stored
    def __init__(self,imageFolder):
        self.imageFolder = imageFolder
        self.trainingFolder = imageFolder + "training/sampled/"

    # given the image media key, use a hash function to determine what subfolder the image is stored in
    # INPUTS:
    #    mediaKey (str) - unique id for the media (image)
    #    nbins (int) - number of bins (i.e. has indexes)
    # OUTPUTS:
    #    name of subfolder the image is stored in
    def hashKey(self,mediaKey,nbins=100):
        return str(abs(int(hashlib.sha512(mediaKey.encode('utf-8')).hexdigest(), 16))%nbins)

    # given json record with flexible structure as input, identify url to download image
    # INPUTS:
    #    record (json) - media information in json format.  Structure can vary
    # OUTPUTS:
    #    mediaKey (str) - unique media id
    #    url (str) - url where image or preview image (e.g. for videos) can  be downloaded
    def getImgUrl(self,record):
        dictKeys = record.keys()
        mediaKey = record['media_key']

        # images can either be previews of gif or videos 'preview_image_url' or actual image 'url'
        if('url' in dictKeys):
            url = record['url']
        else:
            url = record['preview_image_url']
        return(mediaKey,url)

    # extract all media keys and urls from a nested array of metadata records
    # INPUTS:
    #    metaFilename (str) - absolute filepath to npy file containing image meta data
    # OUTPUTS:
    #    imageTuples (tuple array) - list of tuples, where each tuple is (media key, image url)
    def parseImageMeta(self,metaFilename):
        imageSet = np.load(metaFilename,allow_pickle=True)
        imageTuples = []

        # records are stored as nested arrays.  One array = small batch of tweet records
        for imageSubset in imageSet:
            for record in imageSubset:
                imageTuples.append(self.getImgUrl(record))
        return(imageTuples)

    # downlaod image and store on disk
    # INPUTS:
    #    imageUrl (str) - url to image to download
    #    outputFilepath (str) - absolute filepath where image should be stored
    def downloadImage(self,imageUrl,outputFilepath):
        # Open the url image, set stream to True, this will return the stream content.
        r = requests.get(imageUrl, stream = True)

        # Check if the image was retrieved successfully
        if r.status_code == 200:
        # Set decode_content value to True, otherwise the downloaded image file's size will be zero.
            r.raw.decode_content = True
        
            # Open a local file with wb ( write binary ) permission.
            with open(outputFilepath,'wb') as f:
                shutil.copyfileobj(r.raw, f)

    # download a set of images 
    # INPUTS:
    #    imageTuples (tuple array) - list of tuples, where each tuple is (media key, image url)
    def downloadImages(self,imageTuples):
        print("downloading %i images" %(len(imageTuples)))

        for imageTuple in imageTuples:
            outputFolder = self.imageFolder + self.hashKey(imageTuple[0]) + "/"
            if not(os.path.exists(outputFolder)):
                os.mkdir(outputFolder)
            outputFilepath = outputFolder + imageTuple[0] + imageTuple[1][-4:] 

            # download images not in storage
            if not(os.path.exists(outputFilepath)):
                self.downloadImage(imageTuple[1],outputFilepath)

    # given a folder where image metadata is stored, load metadata, process, and download images
    # INPUTS: 
    #    folder (str) - absolute filepath where image metafiles are located
    def downloadImagesFromMetaFolder(self,folder):
        
        # metafiles start with the prefix "me"
        metaFiles = glob.glob(folder + "me_*")
        print("found %i meta files " %(len(metaFiles)))
        for metaFile in metaFiles:
            print("downloading images for meta file %s" %(metaFile))

            # image info is stored with other metadata.  First need to extract image info
            imageTuples = self.parseImageMeta(metaFile)
            self.downloadImages(imageTuples)

    # DEPRECATED???? CHECK
    # find url for a specific media key
    # INPUTS: 
    #    folder (str) - absolute filepath where image metafiles are located
    #    mediaKey (str) - media key to look for
    def findMediaUrl(self,folder,mediaKey):
        
        # metafiles start with the prefix "me"
        metaFiles = glob.glob(folder + "me_*")
        print("found %i meta files " %(len(metaFiles)))
        for metaFile in metaFiles:
            print("downloading images for meta file %s" %(metaFile))

            imageSet = np.load(metaFile,allow_pickle=True)
            imageTuples = []

            # records are stored as nested arrays.  One array = small batch of tweet records
            for imageSubset in imageSet:
                for record in imageSubset:
                    if(record['media_key'] == mediaKey):
                        print(record)
                        return


    # copy images from main store to training subfolder
    # INPUTS:
    #    trainingDict (dictionary) - tweets sampled for training, including image filenammes
    # OUTPUTS:
    #    imgNames (str array) - list of filenames for each tweet (with "SN.png" for tweets with no images attached)
    def copyTrainingImages(self,trainingDict):
        imgNames = []

        # for each tweet in the trainingdataset, get the image filename or append "SN.png" if no image is attached
        # to the tweet
        for record in trainingDict:
            if(record['media_keys']!=["None"]):
                mediaKey = record['media_keys'][0]
                folder = self.imageFolder + record['created_at'][0:4] + "/" + self.hashKey(mediaKey) + "\\"
                candFilename = folder + mediaKey + ".jpg"
                chosenFile = ""
                if(os.path.exists(candFilename)):
                    chosenFile = candFilename
                else:
                    filenames = glob.glob(folder + mediaKey + "*" )
                    if(len(filenames)>0):
                        chosenFile = filenames[0]
                if(len(chosenFile)>0):
                    shortName = chosenFile[chosenFile.rfind("\\") +1:]
                    outFilepath = self.trainingFolder + shortName
                    shutil.copyfile(chosenFile,outFilepath)
                    imgNames.append(shortName)
                else:
                    print("couldn't find image for media key %s" %(mediaKey) )
                    imgNames.append('None')
            else:
                imgNames.append("SN.png")
        return(imgNames)
                    

# end of TweetImageClass.py