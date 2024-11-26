from mySecrets import secrets
from AugmentImage_Class import AugmentImage
import pandas as ps
import os

imageHomeFolder = secrets['imageFolder']
imageTestFolder = secrets['imageTestFolder']
augTrainFolder = secrets['augTrainFolder']
augTestFolder = secrets['augTestFolder']
imageList = secrets['imageList']
imageTrainList = secrets['imageTrainList']
imageTestList = secrets['imageTestList']


def copyTrainTest(augmenter,trainSet,testSet):
    testHttps = list(set(testSet['img_http']))
    for img in testHttps:
        augmenter.copyFile(img,imageTestFolder)

    trainHttps = list(set(trainSet['img_http']))
    for img in trainHttps:
        augmenter.copyFile(img,augTrainFolder)

def augmentTrainImages(imageTrainFolder):

    trainImgs = os.listdir(augmenter,imageHomeFolder)
    for imgName in trainImgs:
        augmenter.augmentImage(imageTrainFolder,imgName,imgName[:-4])

def createAugmentedImageCSV(trainRecords,outputFilepath):
    nRecords = trainRecords.count()[0]
    filename,encoding = [],[]
    for recordNum in range(nRecords):
        curRecord = trainRecords.iloc[recordNum]
        imgBaseName = curRecord['img_http']
        testImgName = imageTestFolder + imgBaseName[:-4] + "_padded.jpg"
        if(os.path.exists(testImgName)):
            filename.append(testImgName)
            encoding.append(trainRecords[recordNum])
    df = ps.DataFrame({
        'filename':filename,
        'encoding':encoding
    })
    df.to_csv(outputFilepath,index=False)


imgDataset = ps.read_csv(imageList)
testSet = imgDataset[imgDataset['test']==1]
trainSet = imgDataset[imgDataset['test']==0]
augmenter = AugmentImage(imageHomeFolder)
copyTrainTest(augmenter,trainSet,testSet)
augmentTrainImages(augTrainFolder)
createAugmentedImageCSV(trainSet,imageTestList)