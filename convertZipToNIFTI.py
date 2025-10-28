#!/usr/bin/env python
# coding: utf-8
## Written by Nikhil Paliwal, circa June 2022. Adapted from Bai et al. Imperial College London

import os
import re
import pickle
import cv2
import pydicom as dicom
import SimpleITK as sitk
import numpy as np
import nibabel as nib
import pandas as pd
import biobank_utils
import glob                    
import dateutil.parser    
from biobank_utils import process_manifest
from biobank_utils import Biobank_Dataset

os.getcwd()

EID=glob.glob('*.zip')
print(len(EID))
eids = []
for i in range(len(EID)):
    sp = (EID[i].split('_'))[0]
    eids.append(sp)

print(len(eids))
eids = list(dict.fromkeys(eids))
print(len(eids))
data_root = os.getcwd()  #('/Users/npaliwal/ukbb_cardiac/bulk0')


for i in range(len(eids)):
    eid = str(eids[i])                                                                                         
                                                                                                                    
    # Destination directories                                                                                       
    data_dir = os.path.join(data_root, eid)                                                                         
    if not os.path.exists(data_dir):                                                                                
        os.mkdir(data_dir)                                                                                          
                                                                                                                    
    dicom_dir = os.path.join(data_dir, 'dicom')                                                                     
    if not os.path.exists(dicom_dir):                                                                               
        os.mkdir(dicom_dir)                                                                                         
                                                                                                                    
    # Create a batch file for this subject                                                                          
    batch_file = os.path.join(data_dir, '{0}_batch'.format(eid))                                                    
    with open(batch_file, 'w') as f_batch:                                                                          
        for j in range(20208, 20210):                                                                               
            # The field ID information can be searched at http://biobank.ctsu.ox.ac.uk/crystal/search.cgi           
            # 20208: Long axis heart images - DICOM Heart MRI                                                       
            # 20209: Short axis heart images - DICOM Heart MRI                                                      
            # 2.0 means the 2nd visit of the subject, the 0th data item for that visit.                             
            # As far as I know, the imaging scan for each subject is performed at his/her 2nd visit.                
            field = '{0}-2.0'.format(j)                                                                             
            f_batch.write('{0} {1}_2_0\n'.format(eid, j))                                                           
                                                                                                                    
    # Unpack the data                                                                                               
    files = glob.glob('{0}_*.zip'.format(eid))                                                                      
    for f in files:                                                                                                 
        os.system('unzip -o {0} -d {1}'.format(f, dicom_dir))                                                       
                                                                                                                    
        # Process the manifest file                                                                                 
        if os.path.exists(os.path.join(dicom_dir, 'manifest.cvs')):                                                 
            os.system('cp {0} {1}'.format(os.path.join(dicom_dir, 'manifest.cvs'),                                  
                                          os.path.join(dicom_dir, 'manifest.csv')))                                 
        process_manifest(os.path.join(dicom_dir, 'manifest.csv'),                                                   
                         os.path.join(dicom_dir, 'manifest2.csv'))                                                  
        df2 = pd.read_csv(os.path.join(dicom_dir, 'manifest2.csv'), on_bad_lines='skip')                          
                                                                                                                    
        # Patient ID and acquisition date                                                                           
        pid = df2.at[0, 'patientid']                                                                                
        date = dateutil.parser.parse(df2.at[0, 'date'][:11]).date().isoformat()                                     
                                                                                                                    
        # Organise the dicom files                                                                                  
        # Group the files into subdirectories for each imaging series                                               
        for series_name, series_df in df2.groupby('series discription'):                                            
            series_dir = os.path.join(dicom_dir, series_name)                                                       
            if not os.path.exists(series_dir):                                                                      
                os.mkdir(series_dir)                                                                                
            series_files = [os.path.join(dicom_dir, x) for x in series_df['filename']]                              
            os.system('mv {0} {1}'.format(' '.join(series_files), series_dir))                                      
                                                                                                                    
    # Convert dicom files and annotations into nifti images                                                         
    dset = Biobank_Dataset(dicom_dir)                                                                               
    dset.read_dicom_images()                                                                                        
    dset.convert_dicom_to_nifti(data_dir)                                                                           
                                                                                                                    
    # Remove intermediate files                                                                                     
    os.system('rm -rf {0}'.format(dicom_dir))                                                                       
    os.system('rm -f {0}'.format(batch_file))                                                                       
    os.system('rm -f {0}_*.zip'.format(eid))                                                                        
                                                                                                                    


##END OF CODE. 
