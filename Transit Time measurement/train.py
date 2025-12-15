import tensorflow_mri as tfmr
import numpy as np
from sklearn.model_selection import train_test_split
from glob import glob
import tensorflow as tf
import tensorflow.keras.backend as K
from neptune.integrations.tensorflow_keras import NeptuneCallback
import scipy
import random
import matplotlib.pyplot as plt
from scipy import ndimage
from loss import *
from unet3plus2 import *

os.environ["CUDA_VISIBLE_DEVICES"] = "0,1"
print("FILE RUNNNING")
 
if __name__ == "__main__":
    print("IN MAIN BODY")
    processed_data_path = ''
    patients = [pat.replace('.npy','') for pat in glob(f'{processed_data_path}/*')] 
    train_patients, val_patients= train_test_split(patients, test_size = 0.25, random_state = 42)
    print(f"{len(train_patients)} training, {len(val_patients)} validation patients.")

    model_name = 'PHAS-66' # Our best model in phase-contrast image segmentation is call 'PHAS-66', which is available here in this folder.
    continue_training = True

    def normalize(image):
        mean = np.mean(image)
        std = np.std(image)
        if std != 0:
            norm = (image - mean) / std
        else:
            norm = np.zeros_like(image)
        return norm
    
    class CustomDataGen3D():    
        def __init__(self, patients, cohort):
            random.shuffle(patients)
            self.patients = patients
            self.cohort = cohort                
            
        def data_generator(self, rotation_range=60): ##################################3D network#######################################
            for patient in self.patients:
                image_mask = np.load(f"{patient}.npy")
                mag = image_mask[..., 0][..., np.newaxis]
                mag_image = normalize(mag)
                mask_values = image_mask[..., 2]

                # Create one-hot encoded channels for each class
                class1_channel = (mask_values == 1).astype(np.float32)  
                class2_channel = (mask_values == 2).astype(np.float32)
                bkg_channel = (mask_values == 0).astype(np.float32)
                # Combine the channels to form mask
                mask = np.stack([bkg_channel, class1_channel, class2_channel], axis=-1)   

                # Apply rotation
                if self.cohort=='train' and random.random() < 0.4:
                    angle = np.random.uniform(-rotation_range, rotation_range)
                    mag_image = ndimage.rotate(mag_image, angle, axes=(1, 2), reshape=False, mode='nearest')
                    mask = ndimage.rotate(mask, angle, axes=(1, 2), reshape=False, mode='nearest')
                mag_final = normalize(mag_image)
                yield mag_final, mask.astype('float32') 
        
        def get_gen(self):
            return self.data_generator() 
    #This part is the defination for Super resolution model
    image_size = None
    input_channel = 1 
    out_channels = 3

    input_shape = [image_size,image_size,image_size,input_channel]
    output_shape = [image_size,image_size,image_size,out_channels]
    train_gen = CustomDataGen3D(train_patients, 'train').get_gen
    val_gen   = CustomDataGen3D(val_patients, 'val').get_gen

    output_signature = (tf.TensorSpec(shape=input_shape, dtype=tf.float32), 
                    tf.TensorSpec(shape=output_shape, dtype=tf.float32))

    train_ds = tf.data.Dataset.from_generator(train_gen, 
                                            output_signature = output_signature)
    val_ds = tf.data.Dataset.from_generator(val_gen, 
                                            output_signature = output_signature)

    batch_size = 1
    train_ds = train_ds.shuffle(66, seed = 42, reshuffle_each_iteration=True).batch(batch_size).prefetch(-1)
    val_ds = val_ds.batch(batch_size).prefetch(-1)

    #This part is the defination for Segmentation model

    def dice_coef(y_true, y_pred, smooth=1e-5): #this code is designed to compute the dice for 3 channels ; mask.shape (1, 32, 192, 192, 3)
        intersection = K.sum(y_true * y_pred, axis=[1, 2, 3])
        union = K.sum(y_true, axis=[1, 2, 3]) + K.sum(y_pred, axis=[1, 2, 3])
        dice = K.mean((2.0 * intersection + smooth) / (union + smooth), axis=0)
        return dice

    def dice_loss(y_true, y_pred):
        loss = 1 - dice_coef(y_true, y_pred)
        return loss

    def focal_tversky_loss(y_true, y_pred, alpha=0.7, beta=0.3, gamma=0.75, smooth=1e-6):
        """
        Focal Tversky loss for multi-class 3D segmentation.

        Args:
        y_true: tensor of shape [B, D, H, W, C]
        y_pred: tensor of shape [B, D, H, W, C]
        alpha: controls the penalty for False Positives
        beta: controls the penalty for False Negatives
        gamma: focal parameter to down-weight easy examples
        smooth: smoothing constant to avoid division by zero

        Returns:
        loss: computed Focal Tversky loss
        """
        y_true = tf.cast(y_true, tf.float32)
        y_pred = tf.clip_by_value(y_pred, smooth, 1.0 - smooth)  # Clipping to avoid log(0)
        
        num_classes = 3
        loss = 0.0
        
        for c in range(num_classes):
            y_true_c = y_true[..., c]
            y_pred_c = y_pred[..., c]
            
            true_pos = tf.reduce_sum(y_true_c * y_pred_c)
            false_neg = tf.reduce_sum(y_true_c * (1 - y_pred_c))
            false_pos = tf.reduce_sum((1 - y_true_c) * y_pred_c)
            
            tversky_index = (true_pos + smooth) / (true_pos + alpha * false_neg + beta * false_pos + smooth)
            loss_c = tf.pow((1 - tversky_index), gamma)
            loss += loss_c
        
        loss /= tf.cast(num_classes, tf.float32)  # Averaging over all classes
        return loss
    
    if continue_training:
        model = tf.keras.models.load_model(f'models/{model_name}', compile = False)
    else: 
        inputs = tf.keras.Input(shape = input_shape)
        unet3 = unet3plus(inputs, 
                    rank = 3,  # dimension 3D Unet
                    n_outputs = 3, 
                    add_dropout = 0, # 1 or 0 to add dropout
                    dropout_rate = 0.3,
                    base_filters = 32, 
                    kernel_size = 3, 
                    pool_size = 2,
                    supervision = False, 
                    CGM = False) 
        model = tf.keras.Model(inputs = inputs, outputs = unet3.outputs())
        
    model.compile( loss=focal_tversky_loss,optimizer=tf.keras.optimizers.Adam(), #default=0.001
                metrics=[dice_coef, tfmr.metrics.F1Score(class_id=1, name='F1AAo'), tfmr.metrics.F1Score(class_id=2, name='F1DAo')])
 
    # Define EarlyStopping callback
    from keras.callbacks import EarlyStopping, ModelCheckpoint

    es = EarlyStopping(monitor='val_dice_coef', #monitoring the dice on validation dataset
                    mode='max', 
                    verbose = 1, 
                    patience = 30) #20

    mc = ModelCheckpoint(f'models/{model_name}',
                        save_best_only= True,
                        monitor='val_dice_coef',  #save the model with max dice (include background)
                        mode='max')

    neptune_callback = NeptuneCallback(run = run)
    model.fit(train_ds,
            validation_data = val_ds, 
            epochs=200,  
            callbacks=[ mc, neptune_callback]) 

