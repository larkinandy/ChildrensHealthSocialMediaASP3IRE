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
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.layers import Conv2D, AveragePooling2D, GlobalAveragePooling2D, Dropout, SpatialDropout2D
from tensorflow.keras import utils
from tensorflow.keras.callbacks import TensorBoard, ModelCheckpoint
import keras.backend as K

# Setting random seeds to reduce the amount of randomness in the neural net weights and results
# The results may still not be exactly reproducible
#np.random.seed(42)
#tf.random.set_seed(42)

BATCH_SIZE = 512
# Testing to ensure GPU is being utilized
# Ensure that the Runtime Type for this notebook is set to GPU
# If a GPU device is not found, change the runtime type under:
# Runtime>> Change runtime type>> Hardware accelerator>> GPU
# and run the notebook from the beginning again.

device_name = tf.test.gpu_device_name()
if device_name != '/device:GPU:0':
    raise SystemError('GPU device not found')
print('Found GPU at: {}'.format(device_name))


# Importing the augmented training dataset and testing dataset to create tensors of images using the filename paths.
train_aug_df = pd.read_csv("/mnt/h/Aspire/faces/face_train.csv")
test_df = pd.read_csv("/mnt/h/Aspire/faces/face_test.csv")
#test_df = test_df.iloc[0:1000]
print(test_df.head())

# Defining a function to return the class labels corresponding to the re-distributed 7 age-ranges.
def class_labels_reassign(age):
    return(age)


train_aug_df['target'] = train_aug_df['age'].map(class_labels_reassign)
test_df['target'] = test_df['age'].map(class_labels_reassign)

print(train_aug_df.groupby(by=['age']).count())

# Converting the filenames and target class labels into lists for augmented train and test datasets.
train_aug_filenames_list = list(train_aug_df['filename'])
train_aug_labels_list = list(train_aug_df['target'])
test_filenames_list = list(test_df['filename'])
test_labels_list = list(test_df['target'])

# Creating tensorflow constants of filenames and labels for augmented train and test datasets from the lists defined above.
train_aug_filenames_tensor = tf.constant(train_aug_filenames_list)
train_aug_labels_tensor = tf.constant(train_aug_labels_list)
test_filenames_tensor = tf.constant(test_filenames_list)
test_labels_tensor = tf.constant(test_labels_list)

# Defining a function to read the image, decode the image from given tensor and one-hot encode the image label class.
# Changing the channels para in tf.io.decode_jpeg from 3 to 1 changes the output images from RGB coloured to grayscale.
num_classes = 7
def _parse_function(filename, label):   
    image_string = tf.io.read_file(filename)
    image_decoded = tf.io.decode_jpeg(image_string, channels=1)    # channels=1 to convert to grayscale, channels=3 to convert to RGB.
    # image_resized = tf.image.resize(image_decoded, [200, 200])
    label2 = tf.one_hot(label, num_classes)
    return(image_decoded,label2)

# Getting the dataset ready for the neural network.
# Using the tensor vectors defined above, accessing the images in the dataset and passing them through the function defined above.
train_aug_dataset = tf.data.Dataset.from_tensor_slices((train_aug_filenames_tensor, train_aug_labels_tensor))
train_aug_dataset = train_aug_dataset.map(_parse_function)
# train_aug_dataset = train_aug_dataset.repeat(3)
train_aug_dataset = train_aug_dataset.batch(BATCH_SIZE)    # Same as batch_size hyperparameter in model.fit() below.
iterator = iter(train_aug_dataset)
optional = iterator.get_next_as_optional()
a = optional.get_value()
print(len(a))
print(a[0].shape)
#print(a[1].shape)
#print(a[2].shape)

test_dataset = tf.data.Dataset.from_tensor_slices((test_filenames_tensor, test_labels_tensor))
test_dataset = test_dataset.map(_parse_function)
# test_dataset = test_dataset.repeat(3)
test_dataset = test_dataset.batch(BATCH_SIZE)    # Same as batch_size hyperparameter in model.fit() below.

# def loadModel():
#     modelFile = '/mnt/h/Aspire/faces/age_model_checkpoint.h5'
#     #modelFile = '/mnt/h/Aspire/a.h5'
#     if(os.path.exists(modelFile)):
#         print('loading preexisting model \n\n\n\n')
#         model = tf.keras.models.load_model(modelFile)
#         print(model.summary())
#         return model
#     else:
#         print("couldn't find previous model \n\n\n\n")

#     # Defining the architecture of the sequential neural network.
#     final_cnn = Sequential()
#     # Input layer with 32 filters, followed by an AveragePooling2D layer.
#     final_cnn.add(Conv2D(filters=32, kernel_size=3, activation='relu', input_shape=(200, 200, 1)))    # 3rd dim = 1 for grayscale images.
#     final_cnn.add(AveragePooling2D(pool_size=(2,2)))
#     final_cnn.add(SpatialDropout2D(0.25))
#     # Three Conv2D layers with filters increasing by a factor of 2 for every successive Conv2D layer.
#     final_cnn.add(Conv2D(filters=64, kernel_size=3, activation='relu'))
#     final_cnn.add(AveragePooling2D(pool_size=(2,2)))
#     final_cnn.add(SpatialDropout2D(0.25))
#     final_cnn.add(Conv2D(filters=128, kernel_size=3, activation='relu'))
#     final_cnn.add(AveragePooling2D(pool_size=(2,2)))
#     final_cnn.add(SpatialDropout2D(0.25))
#     final_cnn.add(Conv2D(filters=256, kernel_size=3, activation='relu'))
#     final_cnn.add(AveragePooling2D(pool_size=(2,2)))
#     final_cnn.add(SpatialDropout2D(0.25))
#     # A GlobalAveragePooling2D layer before going into Dense layers below.
#     # GlobalAveragePooling2D layer gives no. of outputs equal to no. of filters in last Conv2D layer above (256).
#     final_cnn.add(GlobalAveragePooling2D())
#     final_cnn.add(Dropout(0.25))
#     # One Dense layer with 132 nodes so as to taper down the no. of nodes from no. of outputs of GlobalAveragePooling2D layer above towards no. of nodes in output layer below (7).
#     final_cnn.add(Dense(132, activation='relu'))
#     # Output layer with 7 nodes (equal to the no. of classes).
#     final_cnn.add(Dense(7, activation='softmax'))
#     final_cnn.summary()

#     return final_cnn








    #mse = tf.reduce_mean(tf.square(y_pred-y_true))
    #rmse = tf.math.sqrt(mse)
    #return rmse / tf.reduce_mean(tf.square(y_true)) - 1


def build_base(inputs):
    class CustomAccuracy(tf.keras.losses.Loss):
        def __init__(self):
            super().__init__()
        def call(self, y_true, y_pred):
            y_pred2 = tf.argmax(y_pred)
            y_true2 = tf.argmax(y_true)
            diff = tf.math.abs(tf.math.subtract(y_pred2,y_true2))
            maxLoss = tf.where(tf.math.equal(y_pred2,0),1.5,0)
            maxLoss2 = tf.where(tf.math.equal(y_true2,0),1.5,0)
            
            maxLoss = tf.math.maximum(maxLoss,maxLoss2)
            #maxLoss = tf.math.multiply(maxLoss,10)
            loss1 = tf.cast(tf.math.reduce_mean(tf.math.minimum(tf.cast(diff,tf.float32),maxLoss),axis=None),tf.float32)
            cce = tf.keras.losses.CategoricalCrossentropy()
            loss2 = cce(y_true,y_pred)
            return (loss2+loss1)
        

    modelFile = '/mnt/h/Aspire/faces/age_model_checkpoint.h5'
    #     #modelFile = '/mnt/h/Aspire/a.h5'
    if(os.path.exists(modelFile)):
        print('loading preexisting model \n\n\n\n')
        model = tf.keras.models.load_model(modelFile, custom_objects={'CustomAccuracy':CustomAccuracy},compile=False)
        print(model.summary())
        return model
    else:
        print("couldn't find previous model \n\n\n\n")
    x = tf.keras.layers.Conv2D(filters=32, kernel_size=3, activation='relu', input_shape=(200, 200, 1))(inputs)
    x = tf.keras.layers.AveragePooling2D(pool_size=(2,2))(x)
    x = tf.keras.layers.SpatialDropout2D(0.25)(x)
    
    x = tf.keras.layers.Conv2D(filters=64, kernel_size=3, activation='relu')(x)
    x = tf.keras.layers.AveragePooling2D(pool_size=(2,2))(x)
    x = tf.keras.layers.SpatialDropout2D(0.25)(x)

    x = tf.keras.layers.Conv2D(filters=128, kernel_size=3, activation='relu')(x)
    x = tf.keras.layers.AveragePooling2D(pool_size=(2,2))(x)
    x = tf.keras.layers.SpatialDropout2D(0.25)(x)

    x = tf.keras.layers.Conv2D(filters=256, kernel_size=3, activation='relu')(x)
    x = tf.keras.layers.AveragePooling2D(pool_size=(2,2))(x)
    x = tf.keras.layers.SpatialDropout2D(0.25)(x)

    x = tf.keras.layers.GlobalAveragePooling2D()(x)
    x = tf.keras.layers.Dropout(0.25)(x)
    
    x = tf.keras.layers.Dense(132,activation='relu')(x)
    x = tf.keras.layers.Dense(7,activation='softmax',name='categorical')(x)

    #y = tf.keras.layers.Dense(1,name='linear')(x)
    #model = tf.keras.Model(inputs=inputs,outputs = [x,y])
    model = tf.keras.Model(inputs=inputs,outputs=x)
    return(model)






    



def runModel(final_cnn):

    class CustomAccuracy(tf.keras.losses.Loss):
        def __init__(self):
            super().__init__()
        def call(self, y_true, y_pred):
            y_pred2 = tf.argmax(y_pred)
            y_true2 = tf.argmax(y_true)
            diff = tf.math.abs(tf.math.subtract(y_pred2,y_true2))
            maxLoss = tf.where(tf.math.equal(y_pred2,0),1.5,0)
            maxLoss2 = tf.where(tf.math.equal(y_true2,0),1.5,0)
            
            maxLoss = tf.math.maximum(maxLoss,maxLoss2)
            #maxLoss = tf.math.multiply(maxLoss,10)
            loss1 = tf.cast(tf.math.reduce_mean(tf.math.minimum(tf.cast(diff,tf.float32),maxLoss),axis=None),tf.float32)
            cce = tf.keras.losses.CategoricalCrossentropy()
            loss2 = cce(y_true,y_pred)
            return (loss2+loss1)
        

    # Compiling the above created CNN architecture.
    #final_cnn.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
    final_cnn.compile(loss=[CustomAccuracy()],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])
    # Creating a TensorBoard callback object and saving it at the desired location.
    #tensorboard = TensorBoard(log_dir=f"/mnt/h/Aspire/faces/cnn_logs")

    # Creating a ModelCheckpoint callback object to save the model according to the value of val_accuracy.

    checkpoint = ModelCheckpoint(filepath=f"/mnt/h/Aspire/faces/age_model_checkpoint.h5",
                                monitor='val_accuracy',
                                save_best_only=True,
                                save_weights_only=False,
                                verbose=1,
                                initial_value_threshold=  None#0.97828
                                )
                                
    # Fitting the above created CNN model.f
    final_cnn_history = final_cnn.fit(train_aug_dataset,
                                    batch_size=BATCH_SIZE,
                                    validation_data=test_dataset,
                                    epochs=100,
                                    callbacks=[checkpoint],
                                    shuffle=True    # shuffle=False to reduce randomness and increase reproducibility
                                    )


with tf.device('/GPU:0'):
    imgInput = tf.keras.layers.Input(shape=(200,200,1),dtype=tf.float32,name='rdInput')
    baseModel = build_base(imgInput)
    runModel(baseModel)