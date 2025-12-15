
"""
This file contains codes to apply segmentation model on UKB data and get flow curves
(1) Model Inputs: Phase-contrast flow images (which have corresponding localizer images), should be extracted from their compressed format. UKB data consists of 90 DICOM files per folder.
(2) Data Processing: Select the magnitude and phase DICOM images and convert them into NumPy arrays. The segmentation model requires only magnitude images for training, while phase images will be used later to compute flow velocity.
"""
import os
import numpy as np
from glob import glob
import tensorflow as tf
import tensorflow.keras.backend as K
import random
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
from scipy import ndimage
from scipy.ndimage import label, find_objects
from matplotlib import animation
from sklearn.model_selection import train_test_split
from scipy.optimize import curve_fit
from scipy.fftpack import fft, ifft
import pandas as pd

model_name = 'PHAS-66'
model = tf.keras.models.load_model(f'models/{model_name}', compile = False)

# channel==0 is magnitude image and channel==1 is the phase image
# UKB original image shape before padding is (30,192,192,2), needs to be zero-padded to (32,192,192,2) to feed into model structure.

patients = #load the images

def normalize(image): #this operation is applied to the entire image tensor
    mean = np.mean(image)
    std = np.std(image)
    if std != 0:
        norm = (image - mean) / std
    else:
        norm = np.zeros_like(image)
    return norm
    
class CustomDataGen3D():    
    def __init__(self, patients):
        random.shuffle(patients)
        self.patients = patients          
    def data_generator(self, rotation_range=60): ##################################3D network##############
        for patient in self.patients:
            image_mask = np.load(f"{patient}.npy")
            mag = image_mask[..., 0][..., np.newaxis]
            mag_image = normalize(mag)
            yield mag_image
    
    def get_gen(self):
        return self.data_generator()  
    
def get_one_hot(targets, nb_classes):
    res = np.eye(nb_classes)[np.array(targets).reshape(-1)]
    return res.reshape(list(targets.shape)+[nb_classes])

# Function to keep only the largest connected component
def keep_largest_component(binary_image):
    # Label connected components
    labeled_array, num_features = label(binary_image)
    if num_features == 0:
        return binary_image  # Return as-is if no components found

    # Find the sizes of each component
    component_sizes = np.array([(labeled_array == i).sum() for i in range(1, num_features + 1)])
    largest_component_label = np.argmax(component_sizes) + 1  # +1 because labels start at 1

    # Keep only the largest component
    largest_component = (labeled_array == largest_component_label)
    return largest_component.astype(binary_image.dtype)

#Save the segmentation 
for id in patients: 
    X_test = []
    test_gen= CustomDataGen3D([id]).get_gen()
    for X in test_gen:
        X_test.append(X)
    X_test = np.stack(X_test) # X shape (1, 32, 192, 192, 1)

    y_pred = model.predict(X_test)
    y_pred= get_one_hot(np.argmax(y_pred,axis = -1), 3)
    y_pred[0, ..., 1] = keep_largest_component(y_pred[0, ..., 1])
    y_pred[0, ..., 2] = keep_largest_component(y_pred[0, ..., 2])
    mask = np.squeeze(y_pred) #5d to 4d
    print(id, mask.shape)
    np.save('path'+id.split('/')[-1]+'.npy', mask)

#Checking semgnation results
for id in patients: 
    X_test = []
    test_gen= CustomDataGen3D([id]).get_gen()
    for X in test_gen:
        X_test.append(X)
    X_test = np.stack(X_test) # X shape (1, 32, 192, 192, 1)
    y_pred = model.predict(X_test)
    y_pred= get_one_hot(np.argmax(y_pred,axis = -1), 3)
    y_pred[0, ..., 1] = keep_largest_component(y_pred[0, ..., 1])
    y_pred[0, ..., 2] = keep_largest_component(y_pred[0, ..., 2])
    print(id, y_pred.shape)
    fig, ax = plt.subplots(1,1, figsize = (3, 5)) #single plot but using a subplot to call it
    frames = []
    for i in range(y_pred.shape[1]):
        p1 = ax.imshow(X_test[0,i,...,0],cmap = 'gray')
        p2 = ax.imshow(y_pred[0,i,...,1],alpha=y_pred[0,i,...,1] * 0.5,cmap = 'jet')
        p3 = ax.imshow(y_pred[0,i,...,2],alpha=y_pred[0,i,...,2] * 0.5,cmap = 'Blues')
        frames.append([p1,p2,p3])
    fig.tight_layout()
    ani = animation.ArtistAnimation(fig, frames)
    ani.save(f"video_path/{id.split('/')[-1]}.gif", fps=y_pred.shape[1]/3)
    plt.close()

# Calculating the flow curves
def scale_flow(phaseImage):
    new_img=phaseImage*2+(-4096)
    return new_img

def normalize_flow(array):
    max_value = np.max(array)
    if max_value != 0:  # To avoid division by zero
        normalized_array = array / max_value
    else:
        print('max value of flow is 0')
        normalized_array = array
    return normalized_array

def interpolated_curve(flow, n): #intersection of the upslope fit 

    flow_fft = fft(flow) #Fourier transform
    pad_size = n - len(flow)
    flow_fft = np.concatenate([ flow_fft[:len(flow)//2], np.zeros(pad_size, dtype=flow_fft.dtype), flow_fft[len(flow)//2:]])
    interpolated_flow = ifft(flow_fft)
    flowint = np.real(interpolated_flow) #flowint is the Fourier interpolated flow curve, length=n

    sign = np.sum(flowint)
    if sign > 0:
        flowint = flowint
    else:
        flowint = flowint*(-1)

    flowint=normalize_flow(flowint)
    
    flowgrad = np.gradient(flowint, edge_order=1) # Calculate the gradient
    maxgradient = np.max(flowgrad[:round(0.25 * len(flowgrad))]) #find maxgradient in the first 1/4 of the flow curve
    inflectiontime = np.argmax(flowgrad[:round(0.25 * len(flowgrad))])# Find the index of this maxgradient

    flowinflection = flowint[inflectiontime]  #flow curve point (x=inflectiontime, y=flowinflection)
    cmax = flowinflection - (maxgradient * inflectiontime)  # cmax= y- a*x
    
    t_foot = -cmax / maxgradient # Calculate the line intercept with x-axis~
    return t_foot, flowint , inflectiontime,  flowinflection,  maxgradient

def linear_func(x, a, b, s):  # define the line that go through (a,b) with slope s
    return s * (x - a) + b
def plot_all(file_name, ascending, descending, n): 
    t_asc_foot, asc_curves, x_point, y_point, slope = interpolated_curve(ascending, n)
    t_des_foot, des_curves, x_point2, y_point2, slope2 = interpolated_curve(descending, n)
    print("Ascending_x_intercept:", t_asc_foot)
    print("descending_x_intercept:", t_des_foot)
    plt.plot(range(n), asc_curves, color='green', label='Ascending Curve')
    plt.plot(range(n), des_curves, color='blue', label='Descending Curve')
    plt.plot(t_asc_foot, 0, 'bo', color='yellow', markersize=5, label='Point (a, b)')# Plot the point (a, b)
    plt.plot(t_des_foot, 0, 'bo', color='cyan', markersize=5, label='Point (a, b)')# Plot the point (a, b)
    x_values = np.linspace(0, x_point + (n/30), 50)
    y_values = linear_func(x_values, x_point, y_point, slope)
    plt.plot(x_values, y_values, 'r-', label='Ascending slope')
    x_values2 = np.linspace(0, x_point2 + (n/30), 50)
    y_values2 = linear_func(x_values2, x_point2, y_point2, slope2)
    plt.plot(x_values2, y_values2, 'r-', label='Descending slope')
    plt.axhline(0, color='k', linestyle='--', linewidth=1)# Plot the x-axis
    plt.xlabel('X')
    plt.ylabel('Y')
    plt.legend()
    plt.grid(True)
    plt.show()
    print(file_name[:7]+"Intersection with x-axis (X) for ascending: ", t_asc_foot," and for descending: ",t_des_foot)

# Plotting the flow curves
mask_dic = #list_of_filenames
ascend_x_intercept= []
descend_x_intercept= []
#Record the difference between ascending/descending curves -- please note this is the interpolated curve
mask_path= 'path'
phase_path='phase_image_path'


for file_name in mask_dic:
    mask = np.load(os.path.join(mask_path, file_name)) 
    rawPhase = np.load(os.path.join(phase_path, file_name))
    raw_phase = rawPhase[...,1]
    phase= scale_flow(raw_phase)
    ascending=[]
    descending=[]
    for i in range(1,31):
        value_asc = np.sum(np.multiply(mask[i,:,:,1],phase[i,:,:]))
        value_des = np.sum(np.multiply(mask[i,:,:,2],phase[i,:,:]))
        ascending.append(value_asc)
        descending.append(value_des)
    plot_all(file_name, ascending, descending, 900)

def save_info_tocsv(file_name, ascending, descending, n): #this code will not plot anything but info saved should be exact as in the previous plots
    asc_curves, x_point = interpolated_curve(ascending, n)
    des_curves, x_point2 = interpolated_curve(descending, n)
    return file_name, x_point, x_point2

#example of use:
results= []
n=900 #about 30-fold interpolation for 30 frames over a cardiac circle

mask_dic = os.listdir(mask_path)
for file_name in mask_dic:
    mask = np.load(os.path.join(mask_path, file_name)) 
    rawPhase = np.load(os.path.join(phase_path, file_name))
    raw_phase = rawPhase[...,1]
    phase= scale_flow(raw_phase)
    ascending=[]
    descending=[]
    for i in range(1,31):
        value_asc = np.sum(np.multiply(mask[i,:,:,1],phase[i,:,:]))
        value_des = np.sum(np.multiply(mask[i,:,:,2],phase[i,:,:]))
        ascending.append(value_asc)
        descending.append(value_des)
    result = save_info_tocsv(file_name[:7], ascending, descending, n) 
    results.append(result)

df = pd.DataFrame(results, columns=['Eid','ascending_x', 'descending_x'])
# Save DataFrame to CSV
df.to_csv('Results/Flow_insection.csv', index=False)

#Calculate the time difference needs info of normal_interval from dicoms
