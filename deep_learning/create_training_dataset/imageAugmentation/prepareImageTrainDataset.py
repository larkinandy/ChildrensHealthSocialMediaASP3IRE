# prepareImageTrainDataset.py 
# Author: Andrew Larkin

# Summary: create test and augmented train image datasets for deep learning

# import libraries
import pandas as ps
import os

# import custom classes and secrets
from mySecrets import secrets
from AugmentImage_Class import AugmentImage

# create global constants from secrets dictionary
imageHomeFolder = secrets['imageFolder']
imageTestFolder = secrets['imageTestFolder']
augTrainFolder = secrets['augTrainFolder']
augTestFolder = secrets['augTestFolder']
imageList = secrets['imageList']
imageTrainList = secrets['imageTrainList']
imageTestList = secrets['imageTestList']

# copy images from central image repository to repo used for deep learning
# INPUTS:
#    augmenter (AugmentImage object) - instance of the AugmentImage class, for 
#                                      augmenting image datasets
#    trainSet (pandas dataframe) - training records, including image filepath
#    testSet (pandas dataframe) - test records, including image filepath
def copyTrainTest(augmenter,trainSet,testSet):

    # copy test dataset images from central image repo to deep learning repo
    testHttps = list(set(testSet['img_http']))
    for img in testHttps:
        augmenter.copyFile(img,imageTestFolder)

    # copy training datset images to deep learning repo
    trainHttps = list(set(trainSet['img_http']))
    for img in trainHttps:
        augmenter.copyFile(img,augTrainFolder)

# augment training dataset images (e.g. rotate, clip, and change image color)
# INPUTS:
#    imageTrainFolder (str) - folderpath where training images are stored
def augmentTrainImages(imageTrainFolder):

    trainImgs = os.listdir(augmenter,imageTrainFolder)

    # for each training image, augment the training image and stored augmented
    # images in the training dataset folder
    for imgName in trainImgs:
        augmenter.augmentImage(imageTrainFolder,imgName,imgName[:-4])

# create a new training metadata csv file, which includes all augmented training image filepaths
# INPUTS:
#    trainRecords (pandas dataframe) - includes metadata for original unaugmented training records
#    outputFilepath (str) - absolute filepath where updated traiing metadata will be stored
def createAugmentedImageCSV(trainRecords,outputFilepath):
    nRecords = trainRecords.count()[0]
    filename,encoding = [],[]

    # for each original training image, update the training record metadata with augmented images
    # created from the original training image
    for recordNum in range(nRecords):
        curRecord = trainRecords.iloc[recordNum]
        imgBaseName = curRecord['img_http']
        testImgName = imageTestFolder + imgBaseName[:-4] + "_padded.jpg"
        if(os.path.exists(testImgName)):
            filename.append(testImgName)
            encoding.append(trainRecords[recordNum])

    # create a pandas dataframe from the updated metadata and save to csv
    df = ps.DataFrame({
        'filename':filename,
        'encoding':encoding
    })
    df.to_csv(outputFilepath,index=False)

# main function
if __name__ == "__main__":

    # get list of all images, both training and test datset
    imgDataset = ps.read_csv(imageList)

    # partition images into test and training datasets
    testSet = imgDataset[imgDataset['test']==1]
    trainSet = imgDataset[imgDataset['test']==0]

    # create instance of class for augmenting training images
    augmenter = AugmentImage(imageHomeFolder)

    # copy iamges to deep learning repo
    copyTrainTest(augmenter,trainSet,testSet)

    # augment training datset images
    augmentTrainImages(augTrainFolder)

    # update training data metadata and save to csv
    createAugmentedImageCSV(trainSet,imageTestList)

# end of prepareImageTrainDataset.py