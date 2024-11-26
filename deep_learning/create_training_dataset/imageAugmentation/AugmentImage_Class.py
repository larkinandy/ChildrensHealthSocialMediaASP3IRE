import os
from scipy import ndimage
import cv2
import numpy as np
from PIL import Image, ImageOps
import pandas as ps
import shutil
import hashlib # hash is a part of the filepath for storing images in manageable-sized subsets


class AugmentImage():
    def __init__(self,imageHomeFolder):  
       self.imageHomeFolder = imageHomeFolder

    def augmentImage(self,imageTrainFolder,imgPath,imgName):
        if(os.path.exists(imageTrainFolder + imgName + '0.jpg')):
            return
        
        # load image
        img = cv2.imread(self.imageHomeFolder + imgPath)
        img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

        # Augmented image: original image with 40deg rotation.
        img_rot_pos40 = ndimage.rotate(img, 40, reshape=False)
        Image.fromarray(img).save(imageTrainFolder + imgName + '0.jpg')
        Image.fromarray(img_rot_pos40).save(imageTrainFolder + imgName + '1.jpg')
        
        # Augmented image: original image with 20deg rotation.
        img_rot_pos20 = ndimage.rotate(img, 20, reshape=False)
        Image.fromarray(img_rot_pos20).save(imageTrainFolder + imgName + '2.jpg')
        
        # Augmented image: original image with -20deg rotation.
        img_rot_neg20 = ndimage.rotate(img, -20, reshape=False)
        Image.fromarray(img_rot_neg20).save(imageTrainFolder + imgName + '3.jpg')

        # Augmented image: original image with -40deg rotation.
        img_rot_neg40 = ndimage.rotate(img, -40, reshape=False)
        Image.fromarray(img_rot_neg40).save(imageTrainFolder + imgName + '4.jpg')

        # Augmented image: original image flipped laterally.
        img_fliplr = np.fliplr(img)
        Image.fromarray(img_fliplr).save(imageTrainFolder + imgName + '5.jpg')

        # Augmented image: flipped image with 40deg rotation.
        img_fliplr_rot_pos40 = ndimage.rotate(img_fliplr, 40, reshape=False)
        Image.fromarray(img_fliplr_rot_pos40).save(imageTrainFolder + imgName + '6.jpg')

        # Augmented image: flipped image with 20deg rotation.
        img_fliplr_rot_pos20 = ndimage.rotate(img_fliplr, 20, reshape=False)
        Image.fromarray(img_fliplr_rot_pos20).save(imageTrainFolder + imgName + '7.jpg')

        # Augmented image: flipped image with -20deg rotation.
        img_fliplr_rot_neg20 = ndimage.rotate(img_fliplr, -20, reshape=False)
        Image.fromarray(img_fliplr_rot_neg20).save(imageTrainFolder + imgName + '8.jpg')

        # Augmented image: flipped image with -40deg rotation.
        img_fliplr_rot_neg40 = ndimage.rotate(img_fliplr, -40, reshape=False)
        Image.fromarray(img_fliplr_rot_neg40).save(imageTrainFolder + imgName + '9.jpg')

    def padImage(self,imgName):
        # read image
        img = cv2.imread(self.imageTrainFolder + imgName)
        old_image_height, old_image_width, channels = img.shape

        # create new image of desired size and color (blue) for padding
        new_image_width = 280
        new_image_height = 280
        color = (0,0,0)
        result = np.full((new_image_height,new_image_width, channels), color, dtype=np.uint8)

        # compute center offset
        x_center = (new_image_width - old_image_width) // 2
        y_center = (new_image_height - old_image_height) // 2

        # copy img image into center of result image
        result[y_center:y_center+old_image_height, 
        x_center:x_center+old_image_width] = img
        
        # save result
        cv2.imwrite(self.imageTrainFolder + "b_centered.jpg", result)

    
    # given the image media key, use a hash function to determine what subfolder the image is stored in
    # INPUTS:
    #    mediaKey (str) - unique id for the media (image)
    #    nbins (int) - number of bins (i.e. has indexes)
    # OUTPUTS:
    #    name of subfolder the image is stored in
    def hashKey(self,mediaKey,nbins=100):
        return str(abs(int(hashlib.sha512(mediaKey.encode('utf-8')).hexdigest(), 16))%nbins)

    def padding(self,img, expected_size):
        desired_size = expected_size
        delta_width = desired_size[0] - img.size[0]
        delta_height = desired_size[1] - img.size[1]
        pad_width = delta_width // 2
        pad_height = delta_height // 2
        padding = (pad_width, pad_height, delta_width - pad_width, delta_height - pad_height)
        return ImageOps.expand(img, padding)

    def resize_with_padding(self,img, expected_size):
        img.thumbnail((expected_size[0], expected_size[1]))
        delta_width = expected_size[0] - img.size[0]
        delta_height = expected_size[1] - img.size[1]
        pad_width = delta_width // 2
        pad_height = delta_height // 2
        padding = (pad_width, pad_height, delta_width - pad_width, delta_height - pad_height)
        return ImageOps.expand(img, padding)

    def copyFile(self,filename,outFolder):
        imageFilepath = self.imageHomeFolder + self.hashKey(filename[:-4],nbins=5000) + "/" + filename
        if not(os.path.exists(imageFilepath)):
            print("image does not exist: %s" %(imageFilepath))
        else:
            outputFilepath = outFolder + filename[:-4] + "_padded.jpg"
            if not(os.path.exists(outputFilepath)):
                img = Image.open(imageFilepath)
                img = img.convert('RGB')
                img = self.resize_with_padding(img, (280, 280))
                img.save(outputFilepath)