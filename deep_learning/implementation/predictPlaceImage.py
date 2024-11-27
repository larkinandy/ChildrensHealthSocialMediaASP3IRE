# Import dependencies
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import cv2
import os
from zipfile import ZipFile
import time
from datetime import datetime
import itertools
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import Conv2D, AveragePooling2D, GlobalAveragePooling2D, Dropout
from tensorflow.keras import utils 
from tensorflow.keras.callbacks import TensorBoard, ModelCheckpoint
import tensorflow_hub as hub
from mySecrets import secrets
# Setting random seeds to reduce the amount of randomness in the neural net weights and results
# The results may still not be exactly reproducible
np.random.seed(42)
tf.random.set_seed(42)

# Testing to ensure GPU is being utilized
# Ensure that the Runtime Type for this notebook is set to GPU
# If a GPU device is not found, change the runtime type under:
# Runtime>> Change runtime type>> Hardware accelerator>> GPU
# and run the notebook from the beginning again.

device_name = tf.test.gpu_device_name()
if device_name != '/device:GPU:0':
    raise SystemError('GPU device not found')
print('Found GPU at: {}'.format(device_name))

BATCH_SIZE = 64

def convertCodeToNumpy(code):
    encoding = []
    for charIndex in range(len(code)):
        curCar = code[charIndex]
        if curCar =='0':
            encoding.append(0)
        elif curCar =='1':
            encoding.append(1)
    return(encoding)

# Defining a function to read the image, decode the image from given tensor and one-hot encode the image label class.
# Changing the channels para in tf.io.decode_jpeg from 3 to 1 changes the output images from RGB coloured to grayscale.
num_classes = 7
def _parse_function(filename):   
    image_string = tf.io.read_file(filename)
    image_decoded = tf.io.decode_jpeg(image_string, channels=3)    # channels=1 to convert to grayscale, channels=3 to convert to RGB.
    #image_decoded = tf.image.resize(image_decoded, [224, 224])
    image_decoded = tf.cast(image_decoded, dtype=tf.float32)
    
    return(image_decoded)

model = tf.keras.Sequential([
    hub.KerasLayer("https://tfhub.dev/google/imagenet/resnet_v2_101/feature_vector/5",
               trainable=False, arguments=dict(batch_norm_momentum=0.997)),
    tf.keras.layers.Dense(100,activation='relu'),
    tf.keras.layers.Dense(7,activation='sigmoid')
])
model.compile(
    loss=[tf.keras.losses.BinaryCrossentropy(from_logits=True)],
    optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])#,run_eagerly=True)
weightFile = secrets['PLACE_IMAGE_MODEL_WEIGHTS']
checkpoint = ModelCheckpoint(filepath=weightFile,
                                monitor='val_loss',
                                save_best_only=True,
                                save_weights_only=True,
                                verbose=1,
                                initial_value_threshold=  None#0.97828
                                )
model(np.zeros((1,224,224,3)))
if(os.path.exists(weightFile)):
    model.load_weights(weightFile)


def mapOrigFilepath(filename):
    return filename[:-11] + ".jpg"

def loadImgs(imgFilepaths):
    arr = list(map(_parse_function,imgFilepaths))
    arr = np.asarray(arr)
    return arr

def prepFilepaths(foldername):
    
    folderToProcess = secrets['PLACE_IMAGE_INPUT_FOLDER'] + foldername
    
    def mapFullFilepath(filename):
        return folderToProcess + '/' + filename
    
    filenames = os.listdir(folderToProcess)
    filesToProcess = list(map(mapFullFilepath,filenames))
    origFilenames = list(map(mapOrigFilepath,filenames))
    return([filesToProcess,origFilenames])

def createCSV(foldername,predictions,origFilenames):
    df = pd.DataFrame(data = predictions,  
                  columns = ['isDaycare','isPark','isHome','isSchool','isNeighborhood','isIndoor','isOutdoor'])
    df['filename'] = origFilenames
    df.to_csv(secrets['PLACE_IMAGE_PRED_FOLDER'] + foldername + ".csv",index=False)

def processSingleFolder(foldername):
    outputFilename = secrets['PLACE_IMAGE_PRED_FOLDER'] + foldername + '.csv'
    if (os.path.exists(outputFilename)):
        print("%s already exists" %(outputFilename))
        return
    filesToProcess,origFilenames = prepFilepaths(foldername)
    imgs = loadImgs(filesToProcess)
    preds = model.predict(imgs)
    createCSV(foldername,preds,origFilenames)

foldersToProcess = os.listdir(secrets['PLACE_IMAGE_INPUT_FOLDER'])
for folder in foldersToProcess:
    processSingleFolder(str(folder))