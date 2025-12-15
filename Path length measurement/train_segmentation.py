import tensorflow_mri as tfmr
import numpy as np
from sklearn.model_selection import train_test_split
from glob import glob
import tensorflow as tf
import tensorflow.keras.backend as K
import random
import matplotlib.pyplot as plt
from skimage import exposure
import scipy
import os
from matplotlib import animation
import neptune
from neptune.new.integrations.tensorflow_keras import NeptuneCallback

os.environ["CUDA_VISIBLE_DEVICES"] = "0,1"
print("FILE RUNNNING")
 
if __name__ == "__main__":
    
    processed_data_path = './' #image cropped around aorta, into shape (144, 116, 96)
    patients = [pat.replace('.npy','') for pat in glob(f'{processed_data_path}/*')] 
    train_patients, val_patients= train_test_split(patients, test_size = 0.2, random_state = 42)
    print(f"{len(train_patients)} training, {len(val_patients)} validation patients.")
 
    continue_training = False
    model_name = 'AOR-93'

    if continue_training:
        run = neptune.init_run(
        project="",
        api_token="",# credentials, monitoring training process/results on neptune
        with_id = model_name)  
    
    else:
        run = neptune.init_run(
        project="",
        api_token="",
        )  
        model_name = list(run.__dict__.values())[-10]

    class CustomDataGen():    
        def __init__(self, patients, cohort):
            random.shuffle(patients)
            self.patients = patients
            self.cohort = cohort                
        def data_generator(self):
            for patient in self.patients:
                image_mask = np.load(f"{patient}.npy")
                image = image_mask[...,0]
                mask = image_mask[...,1]
                image=normalize(image)
                image= aug_down_gamma_up(image)
                image=image[...,np.newaxis]
                mask=mask[...,np.newaxis]
                bkg = np.zeros(mask.shape[:2])
                bkg = np.where(np.sum(mask,-1) == 1, 0, 1)
                mask = np.concatenate([bkg[...,np.newaxis],mask], -1)
                image=normalize(image)
                yield image, mask.astype('uint8')           
        def get_gen(self):
            return self.data_generator() 	 
            
    def normalize(image):
        mean = np.mean(image)
        std = np.std(image)
        if std != 0:
            norm = (image - mean) / std
        else:
            norm = np.zeros_like(image)
        return norm

    def random_gamma(img2):
        num_slice = random.randint(0, 3) 
        if num_slice != 0:
            start_slice = random.randint(0, img2.shape[0] - 4)
            selected_slices = list(range(start_slice, start_slice + num_slice))
            #print(selected_slices) 
            for i in range(num_slice):
                slice_i = img2[selected_slices[i], :, :]
                slice_i = np.clip(slice_i, 0, None)
                gamma=round(np.random.beta(1, 5) * 0.2 + 0.5, 1) 
                ad_slice = exposure.adjust_gamma(slice_i, gamma)
                img2[selected_slices[i], :, :] = ad_slice
        return img2

    def random_dark(image):
        num_slice = random.randint(0, 2) 
        if num_slice != 0:
            start_slice = random.randint(0, image.shape[0] - 5)
            selected_slices = list(range(start_slice, start_slice + num_slice))
            #print(selected_slices)
            for i in range(num_slice):
                slice_i = image[selected_slices[i], :, :]
                slice_i = np.clip(slice_i, 0, None)
                gamma=round(random.uniform(1.2, 1.7), 1) 
                ad_slice = exposure.adjust_gamma(slice_i, gamma)
                image[selected_slices[i], :, :] = ad_slice
        return image

    def aug_down_gamma_up(image):
        resolu= round(random.uniform(5.9, 7.5), 1)
        adj_image=scipy.ndimage.zoom(image, (1/resolu,1,1), order=1, mode='constant')
        gamma_image=random_gamma(adj_image)
        dark_image=random_dark(gamma_image)
        image3d=scipy.ndimage.zoom(dark_image, (resolu,1,1), order=3, mode='constant')
        if image3d.shape[0] >= 144:
            image3d= image3d[:144, :, :] # Crop the bottom
        elif image3d.shape[0] < 144:
            bottom_pad = 144 - image3d.shape[0]
            image3d = np.pad(image3d, ((0, bottom_pad), (0, 0), (0, 0)), mode='constant', constant_values=0)

        #print(image3d.shape)
        return image3d
    
    output_channel=2
    batch_size = 1
    input_shape = [None,None,None,1]
    output_shape = [None,None,None,output_channel] #output channel=2

    train_gen = CustomDataGen(train_patients, 'train').get_gen
    val_gen   = CustomDataGen(val_patients, 'val').get_gen

    output_signature = (tf.TensorSpec(shape=input_shape, dtype=tf.float32), tf.TensorSpec(shape=output_shape, dtype=tf.float32))

    train_ds = tf.data.Dataset.from_generator(train_gen, output_signature = output_signature)
    val_ds = tf.data.Dataset.from_generator(val_gen, output_signature = output_signature)

    train_ds = train_ds.shuffle(60, seed = 42, reshuffle_each_iteration=True).batch(batch_size).prefetch(-1)
    val_ds = val_ds.batch(batch_size).prefetch(-1)

    def iou(y_true, y_pred, dtype=tf.float32):
        # tf tensor casting
        y_pred = tf.convert_to_tensor(y_pred)
        y_pred = tf.cast(y_pred[...,1:], dtype)
        y_true = tf.cast(y_true[...,1:], y_pred.dtype)

        y_pred = tf.squeeze(y_pred)
        y_true = tf.squeeze(y_true)
        
        y_true_pos = tf.reshape(y_true, [-1])
        y_pred_pos = tf.reshape(y_pred, [-1])

        area_intersect = tf.reduce_sum(tf.multiply(y_true_pos, y_pred_pos))
        
        area_true = tf.reduce_sum(y_true_pos)
        area_pred = tf.reduce_sum(y_pred_pos)
        area_union = area_true + area_pred - area_intersect
        
        return tf.math.divide_no_nan(area_intersect, area_union)

    def dice_coef(y_true, y_pred, const=K.epsilon()):
        
        # flatten 2-d tensors
        y_true_pos = tf.reshape(y_true[...,1:], [-1])
        y_pred_pos = tf.reshape(y_pred[...,1:], [-1])
        
        # get true pos (TP), false neg (FN), false pos (FP).
        true_pos  = tf.reduce_sum(y_true_pos * y_pred_pos)
        false_neg = tf.reduce_sum(y_true_pos * (1-y_pred_pos))
        false_pos = tf.reduce_sum((1-y_true_pos) * y_pred_pos)
        
        # 2TP/(2TP+FP+FN) == 2TP/()
        coef_val = (2.0 * true_pos + const)/(2.0 * true_pos + false_pos + false_neg)
        
        return coef_val

    if continue_training:
        model = tf.keras.models.load_model(f'models/{model_name}', compile = False)
    else:
        inputs = tf.keras.Input(shape = [None,None,None,1]) #define input_shape don't need to specify batch size
        tf.keras.backend.clear_session()
        model = tfmr.models.UNet3D (filters=[64,128,256],
                            kernel_size=3,
                            out_activation='softmax',
                            out_channels = output_channel,
                            use_batch_norm=True)

    model.compile(loss='categorical_crossentropy',
                optimizer=tf.keras.optimizers.Adam(learning_rate=0.0005), metrics=[dice_coef,iou])

    neptune_callback = NeptuneCallback(run=run) 
    num_epochs = 200

    from keras.callbacks import EarlyStopping, ModelCheckpoint
    es = EarlyStopping(monitor='val_loss', 
                    mode='auto', 
                    verbose = 1, 
                    patience = 30)
    mc = ModelCheckpoint(f'models/{model_name}',
                        save_best_only= True,
                        monitor='val_loss',     #Here change the monitor form 'loss' in PHAS-12 to 'dice_coef'
                        mode='min')
    model.fit(train_ds,
            validation_data = val_ds, 
            epochs=num_epochs,
            callbacks=[es, mc, neptune_callback])
    
    #save the segmentation results
    if continue_training:
        model = tf.keras.models.load_model(f'models/{model_name}', compile = False)
    print(model_name)

    def get_one_hot(targets, nb_classes):
        res = np.eye(nb_classes)[np.array(targets).reshape(-1)]
        return res.reshape(list(targets.shape)+[nb_classes])
    
    class CustomDataGenTest():    
        def __init__(self, patients,cohort):
            random.shuffle(patients)
            self.patients = patients
            self.cohort = cohort                
        def data_generator(self):
            for patient in self.patients:
                image= np.load(f"{patient}.npy")
                yield normalize(image) #normalising 
        def get_gen(self):
            return self.data_generator()

    test_patients = [pat.replace('.npy','') for pat in glob('test_data/*')]       #size (144, 116, 96)

    from matplotlib import animation
    from skimage.measure import label   
    from scipy.ndimage import binary_erosion, binary_dilation 
    import time

    def getLargestCC(segmentation): #Only works on 3D masks!
        labels = label(segmentation)
        assert( labels.max() != 0 ) # assume at least 1 CC
        largestCC = labels == np.argmax(np.bincount(labels.flat)[1:])+1
        return largestCC

    def dilation_erosion(mask_aorta): #Only works on 3D images!
        structure = np.ones((3, 3, 3))  
        dilated_mask = binary_dilation(mask_aorta, iterations=3, structure=structure).astype(mask_aorta.dtype)# Perform erosion
        good_mask = binary_erosion(dilated_mask, iterations=2, structure=structure).astype(mask_aorta.dtype)# Perform dilation
        return good_mask

    def clean_mask_final(mask):  #return the clean results as the same number of channels as the input.
        mask = get_one_hot(np.argmax(mask,axis = -1), 2)
        mask = np.squeeze(mask) #5d to 4d
        mask = mask[...,1] #4d to 3d
        mask2 = dilation_erosion(mask)
        fin_mask = getLargestCC(mask2) #clean
        return fin_mask

    for id in test_patients: 
        X_test = []
        test_gen= CustomDataGenTest([id], 'test').get_gen()
        for X in test_gen:
            X=X[:,:,:,np.newaxis]
            X_test.append(X)
        X_test = np.stack(X_test)
        y_pred = model.predict(X_test)

        mask2save = clean_mask_final(y_pred)
        print(mask2save.shape)
        np.save('data/results/'+id.split('/')[-1]+'.npy', mask2save)

