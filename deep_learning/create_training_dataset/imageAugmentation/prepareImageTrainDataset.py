from mySecrets import secrets
from AugmentImage_Class import AugmentImage
import pandas as ps

imageHomeFolder = mySecrets['imageFolder']
imageTestFolder = mySecrets['imageTestFolder']
augTrainFolder = mySecrets['augTrainFolder']
augTestFolder = mySecrets['augTestFolder']
imageList = mySecrets['imageList']
imageTrainList = mySecrets['imageTrainList']
imageTestList = mySecrets['imageTestList']

def copyFile(filename,outFolder):
    imageFilepath = imageHomeFolder + hashKey(filename[:-4],nbins=5000) + "/" + filename
    if not(os.path.exists(imageFilepath)):
        print("image does not exist: %s" %(imageFilepath))
    else:
        outputFilepath = outFolder + filename[:-4] + "_padded.jpg"
        if not(os.path.exists(outputFilepath)):
            img = Image.open(imageFilepath)
            img = img.convert('RGB')
            img = resize_with_padding(img, (280, 280))
            img.save(outputFilepath)

def createAugImgCSV(imgSet,labels,imgFolder,outputFilepath):
    filename,encoding = [],[]
    nRecords = imgSet.count()[0]
    for recordNum in range(nRecords):
        curRecord = imgSet.iloc[recordNum]
        imgBaseName = curRecord['img_http']
        if(os.path.exists(augTrainFolder  + imgBaseName[:-4] + "_padded0.jpg")):
            paddedBase = imgBaseName[:-4] + "_padded"
            for curNum in range(10):
                curImgName = paddedBase + str(curNum) + ".jpg"
                filename.append(augTrainFolder + curImgName)
                encoding.append(labels[recordNum])
    augDF = ps.DataFrame({
        'filename':filename,
        'encoding':encoding
    })
    augDF.to_csv(outputFilepath,index=False)

def copyTrainTest(trainSet,testSet):
    testHttps = list(set(testSet['img_http']))
    for img in testHttps:
        copyFile(img,imageTestFolder)

    trainHttps = list(set(trainSet['img_http']))
    for img in trainHttps:
        copyFile(img,imageTrainFolder)

def augmentTrainImages(imageTrainFolder):

    trainImgs = os.listdir(imageTrainFolder)
    for imgName in trainImgs:
        img = cv2.imread(imageTrainFolder + imgName)
        img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
        imageAugmenter.augmentImage(img,imgName[:-4])

def creatAugmentedImageCSV(trainRecords,outputFilepath):
    nRecords = trainRecords.count()[0]
    filename,encoding = [],[]
    for recordNum in range(nRecords):
        curRecord = trainRecords.iloc[recordNum]
        imgBaseName = curRecord['img_http']
        testImgName = imageTestFolder + imgBaseName[:-4] + "_padded.jpg"
        if(os.path.exists(testImgName)):
            filename.append(testImgName)
            encoding.append(tweetLabels3[recordNum])
    df = ps.DataFrame({
        'filename':filename,
        'encoding':encoding
    })
    df.to_csv(outputFilepath,,index=False)



imageAugmenter = AugmentImage(augTrainFolder)
imgDataset = ps.read_csv(imageList)
testSet = imgDataset[imgDataset['test']==1]
trainSet = imgDataset[imgDataset['test']==0]
copyTrainTest(trainSet,testSet)
augmentTrainImages(imageTrainFolder)
createAugmentedImageCSV(trainRecords,imageTestList)