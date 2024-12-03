# predictPlaceImge.py
# Author: Andrew Larkin

# Summary: predict place labels from social media imagery

# Import dependencies
import pandas as pd
import numpy as np
import os
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.callbacks import ModelCheckpoint
import tensorflow_hub as hub
from mySecrets import secrets


# load image from file and convert to grayscale
# INPUTS:
#    filename (str) - absolute filepath to image
# OUTPUTS:
#    image_decoded (tensor) - image values stored as a tf tensor
def _parse_function(filename):   
    image_string = tf.io.read_file(filename)
    image_decoded = tf.io.decode_jpeg(image_string, channels=3)    # channels=1 to convert to grayscale, channels=3 to convert to RGB.
    image_decoded = tf.cast(image_decoded, dtype=tf.float32)
    
    return(image_decoded)

# convert string sequence of 0s and 1s to a binary numpy array
# INPUTS:
#    code (str) - string sequence of 0s and 1s
# OUTPUTS:
#    encoding (np integer array) - numpy array of 0s and 1s
def convertCodeToNumpy(code):
    encoding = []
    for charIndex in range(len(code)):
        curCar = code[charIndex]
        if curCar =='0':
            encoding.append(0)
        elif curCar =='1':
            encoding.append(1)
    return(encoding)

# given a filename, determine the original filename before name was appended with metadata
# INPUTS:
#    filename (str) - absolute filepath
# OUTPUTS:
#    root filename (str)
def mapOrigFilepath(filename):
    return filename[:-11] + ".jpg"

# load images from disk to numpy array
# INPUTS:
#    imgFilepaths (str) - absolute filepaths to imagery
# OUTPUTS:
#    arr (numpy matrix) - contains set of 2d imagery in np arrays
def loadImgs(imgFilepaths):
    arr = list(map(_parse_function,imgFilepaths))
    arr = np.asarray(arr)
    return arr

# given folderpath, create list of absolute filepaths for files in the folder
# INPUTS:
#    foldername (str) - folder name containing files to process. Not folderpath!
# OUTPUTS:
#    filesToProcess (str list) - absolute filepaths to files to process
#    origFilenames (str list) - original filenames before names were appended with metadata
def prepFilepaths(foldername):
    
    folderToProcess = secrets['PLACE_IMAGE_INPUT_FOLDER'] + foldername
    
    # given relative filepath, return absolute filepath
    # INPUTS:
    #    filename (str) - relative filepath
    # OUTPUTS:
    #    absolute filepath (str)
    def mapFullFilepath(filename):
        return folderToProcess + '/' + filename
    
    filenames = os.listdir(folderToProcess)
    filesToProcess = list(map(mapFullFilepath,filenames))
    origFilenames = list(map(mapOrigFilepath,filenames))
    return([filesToProcess,origFilenames])

# create csv containing filenames and model predictions 
# INPUTS:
#    foldername (str) - folder that was processed. Includes metadata including year and category
#    predictions (np matrix) - model predictions for each label
#    origFilenames (str array) - image names that correspond to model predictions
def createCSV(foldername,predictions,origFilenames):
    df = pd.DataFrame(data = predictions,  
                  columns = ['isDaycare','isPark','isHome','isSchool','isNeighborhood','isIndoor','isOutdoor'])
    df['filename'] = origFilenames
    df.to_csv(secrets['PLACE_IMAGE_PRED_FOLDER'] + foldername + ".csv",index=False)

# generate predictions for all images within a folder
# INPUTS:
#    foldername (str) - name of folder containing images. Does not include folderpath!
def processSingleFolder(foldername):
    outputFilename = secrets['PLACE_IMAGE_PRED_FOLDER'] + foldername + '.csv'
    if (os.path.exists(outputFilename)):
        print("%s already exists" %(outputFilename))
        return
    filesToProcess,origFilenames = prepFilepaths(foldername)
    imgs = loadImgs(filesToProcess)
    preds = model.predict(imgs)
    createCSV(foldername,preds,origFilenames)

# main function
if __name__ == '__main__':

    # Testing to ensure GPU is being utilized
    device_name = tf.test.gpu_device_name()
    if device_name != '/device:GPU:0':
        raise SystemError('GPU device not found')
    print('Found GPU at: {}'.format(device_name))

   # number of labels in the place model
    num_classes = 7
    # number of images per batch
    BATCH_SIZE = 64

    # create model, compile, and load weights
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

    # get list of folders to process. Predict labels for one folder at a time
    foldersToProcess = os.listdir(secrets['PLACE_IMAGE_INPUT_FOLDER'])
    for folder in foldersToProcess:
        processSingleFolder(str(folder))