# trainFaceModel.py
# Author: Andrew Larkin

# Summary: train deep learning model to predict child development age based on imagery

# import libraries
import pandas as ps
import os
import tensorflow as tf
from tensorflow.keras.callbacks import ModelCheckpoint

# import secrets
from mySecrets import secrets

# custom loss metric. Add additional loss if the model predicts there is at least one child present when there are no children present,
# and vice versa
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
            loss1 = tf.cast(tf.math.reduce_mean(tf.math.minimum(tf.cast(diff,tf.float32),maxLoss),axis=None),tf.float32)
            cce = tf.keras.losses.CategoricalCrossentropy()
            loss2 = cce(y_true,y_pred)
            return (loss2+loss1)


# train cnn model to predict age group from faces
# INPUTS:
#    cnn (tf model) - custom cnn model architecture
#    trainDataset (tf data loader) - loads training data in batch sizes
#    testDataset (tf data loader) - loads test data in batch sizes
def trainModel(cnn,trainDataaset,testDataset):

    # Compiling the above created CNN architecture.
    cnn.compile(loss=[CustomAccuracy()],optimizer=tf.keras.optimizers.Adam(learning_rate=0.0001),metrics=['accuracy'])

    # Creating a ModelCheckpoint callback object to save the model according to the value of val_accuracy.
    checkpoint = ModelCheckpoint(
        filepath=secrets['FACE_MODEL_FILEPATH'],
        monitor='val_accuracy',
        save_best_only=True,
        save_weights_only=False,
        verbose=1,
        initial_value_threshold=  None#0.97828
    )
                                
    # Fitting the above created CNN model.f
    cnn.fit(
        trainDataaset,
        batch_size=BATCH_SIZE,
        validation_data=testDataset,
        epochs=100,
        callbacks=[checkpoint],
        shuffle=True    # shuffle=False to reduce randomness and increase reproducibility
    )

# Defining a function to return the class labels corresponding to the re-distributed 7 age-ranges.
def class_labels_reassign(age):
    return(age)

# define the cnn architecture
# INPUTS:
#    inputs (tf.Keras.layer) - defines the size of the model inputs 
# OUTPUTS:
#    model (tf.keras.Model) - model architecture
def build_base(inputs):        

    # if model architecture has arlready been defined, then load the model architecture. OW define the architecture
    modelFile = secrets['FACE_MODEL_FILEPATH']
    if(os.path.exists(modelFile)):
        print('loading preexisting model \n\n\n\n')
        model = tf.keras.models.load_model(modelFile, custom_objects={'CustomAccuracy':CustomAccuracy},compile=False)
        print(model.summary())
        return model
    else:
        print("couldn't find previous model \n\n\n\n")

    # 4 layers of convoluion, average pooling, and dropout
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

    # one final layer of average pooling, then fully connected layers and condensation to 7 outputs
    x = tf.keras.layers.GlobalAveragePooling2D()(x)
    x = tf.keras.layers.Dropout(0.25)(x)
    
    x = tf.keras.layers.Dense(132,activation='relu')(x)

    # each face can only belong to one age group category
    x = tf.keras.layers.Dense(7,activation='softmax',name='categorical')(x)

    model = tf.keras.Model(inputs=inputs,outputs=x)
    return(model)

# Define a function to read the image, decode the image from given tensor and one-hot encode the image label class.
# Change the channels para in tf.io.decode_jpeg from 3 to 1 changes the output images from RGB coloured to grayscale.
def _parse_function(filename, label):   
        image_string = tf.io.read_file(filename)
        image_decoded = tf.io.decode_jpeg(image_string, channels=1)    # channels=1 to convert to grayscale, channels=3 to convert to RGB.
        label2 = tf.one_hot(label, num_classes)
        return(image_decoded,label2)

# main function
if __name__ == '__main__':

    BATCH_SIZE = 512
    num_classes = 7
    
    # Testing to ensure GPU is being utilized
    device_name = tf.test.gpu_device_name()
    if device_name != '/device:GPU:0':
        raise SystemError('GPU device not found')
    print('Found GPU at: {}'.format(device_name))


    # import the augmented training dataset and testing dataset
    train_aug_df = ps.read_csv(secrets['FACE_TRAIN_FILEPATH'])
    test_df = ps.read_csv(secrets['FACE_TEST_FILEPATH'])

    # create column to store class labels
    train_aug_df['target'] = train_aug_df['age'].map(class_labels_reassign)
    test_df['target'] = test_df['age'].map(class_labels_reassign)

    # get list of inputs (filenames) and outputs (labels)
    train_aug_filenames_list = list(train_aug_df['filename'])
    train_aug_labels_list = list(train_aug_df['target'])
    test_filenames_list = list(test_df['filename'])
    test_labels_list = list(test_df['target'])

    # Create tensorflow constants of filenames and labels
    train_aug_filenames_tensor = tf.constant(train_aug_filenames_list)
    train_aug_labels_tensor = tf.constant(train_aug_labels_list)
    test_filenames_tensor = tf.constant(test_filenames_list)
    test_labels_tensor = tf.constant(test_labels_list)


    # load training images from filenames into memory.
    train_aug_dataset = tf.data.Dataset.from_tensor_slices((train_aug_filenames_tensor, train_aug_labels_tensor))
    train_aug_dataset = train_aug_dataset.map(_parse_function)
    train_aug_dataset = train_aug_dataset.batch(BATCH_SIZE)    # Same as batch_size hyperparameter in model.fit() below.

    # load test images from filenames into memory
    test_dataset = tf.data.Dataset.from_tensor_slices((test_filenames_tensor, test_labels_tensor))
    test_dataset = test_dataset.map(_parse_function)
    test_dataset = test_dataset.batch(BATCH_SIZE)    # Same as batch_size hyperparameter in model.fit() below.

    # create and train cnn model
    with tf.device('/GPU:0'):
        imgInput = tf.keras.layers.Input(shape=(200,200,1),dtype=tf.float32,name='rdInput')
        baseModel = build_base(imgInput)
        trainModel(baseModel,train_aug_dataset,test_dataset)

# end of trainFaceModel.py