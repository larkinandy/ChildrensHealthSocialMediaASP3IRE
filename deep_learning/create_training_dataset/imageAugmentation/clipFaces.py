import sys
import os
from mtcnn import MTCNN
from PIL import Image
import numpy as np
import cv2
import pandas as ps
from mySecrets import secrets

def processMTCNN_Jpeg(results,img,outputFile):
    if(len(results)==0):
        return
    index=0
    for result in results:
        # extract the bounding box from the first face
        x1, y1, width, height = result['box']
        # bug fix
        x1, y1 = abs(x1), abs(y1)
        x2, y2 = x1 + width, y1 + height
        if(width>=50 and height >= 50):
        # extract the face
            face = img[y1:y2, x1:x2]
            try:
                image = Image.fromarray(face)
                image = image.resize((200, 200))
                image.save(outputFile + str(index) + ".jpg")
                index+=1
            except Exception as e:
                print(str(e))
                print(face)

def processSingleImg(imgFolder,imgName,outFolder):
    outFilepath = outFolder + imgName[:-4] + "m"
    if not os.path.exists(outFilepath + "0.jpg"):
        try:
            img = cv2.cvtColor(cv2.imread(imgFolder + imgName), cv2.COLOR_BGR2RGB)
            results = detector.detect_faces(img)
            processMTCNN_Jpeg(results,img,outFilepath)
        except Exception as e:
            print(str(e))

def processSingleFolderJpeg(curFolder,outputFolder):
    imgsToProcess = os.listdir(curFolder)
    for img in imgsToProcess:
        print(img)
        processSingleImg(curFolder,img,outputFolder)

detector = MTCNN(steps_threshold=[0.7, 0.7, 0.7])
processSingleFolderJpeg(secrets['imageTestFolder'],secrets['imageFaceTestFolder'])