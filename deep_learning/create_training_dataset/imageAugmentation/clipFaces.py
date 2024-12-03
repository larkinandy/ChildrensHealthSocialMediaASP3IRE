# clipFaces.py
# Author: Andrew Larkin

# Summary: clip faces from imagery and store each image as a new jpb

# import libraries
import sys
import os
from mtcnn import MTCNN
from PIL import Image
import numpy as np
import cv2
import pandas as ps
from mySecrets import secrets

# given an image and coordinates of faces found in the image, clip out of the faces
# and save each face as a new jpb
# INPUTS:
#    results (list of dicts) - each dict contains coordinates of a single faces found in the image
#    img (PIL image object) - image loaded into memory
#    outputFile (str) - absolute filepath where faces should be stored, without the file extension
def processMTCNN_Jpeg(results,img,outputFile):
    if(len(results)==0):
        return
    index=0

    # for each face, clip out of the face and save
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

# find faces in an image, clip out the faces, and save faces as new jpgs
# INPUTS:
#    imgFolder (str) - folderpath where the image is located
#    imgName (str) - filename of the image to analyze
#    outFolder (str) - folderpath where clipped faces should be saved
def processSingleImg(imgFolder,imgName,outFolder):
    outFilepath = outFolder + imgName[:-4] + "m"
    if not os.path.exists(outFilepath + "0.jpg"):
        try:
            # load image into memory
            img = cv2.cvtColor(cv2.imread(imgFolder + imgName), cv2.COLOR_BGR2RGB)

            # ideentify faces in image
            results = detector.detect_faces(img)

            # clip out the faces and save to disk
            processMTCNN_Jpeg(results,img,outFilepath)
        except Exception as e:
            print(str(e))

# create a list of images within a folder and analyze all images in the list
# INPUTS:
#    curFolder (str) - folderpath where images are stored
#    outputFolder (str) - folderpath where clipped images will be saved
def processSingleFolderJpeg(curFolder,outputFolder):
    imgsToProcess = os.listdir(curFolder)

    # for each image in the folder, find the faces, clip them out, and save to disk
    for img in imgsToProcess:
        print(img)
        processSingleImg(curFolder,img,outputFolder)


# main function
if __name__ == "__main__":
    detector = MTCNN(steps_threshold=[0.7, 0.7, 0.7])
    processSingleFolderJpeg(secrets['imageTestFolder'],secrets['imageFaceTestFolder'])

# end of clipFaces.py