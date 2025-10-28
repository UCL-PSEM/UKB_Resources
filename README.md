## Overview
This is a step-by-step guide on how to extract cardiac traits from the UK Biobank. These steps are developed by Nikhil 
Paliwal (n.paliwal@ucl.ac.uk) circa summer 2022. Prior understanding of the **ukbb_cardiac** repository 
(https://github.com/baiwenjia/ukbb_cardiac) is required to execute these steps.

**Note** You will need access to the UK Biobank application and a key file that allows you to download the bulk data; 
cardiac imaging data is considered bulk by UKB.

## Downloading the cardiac MRI images

The cardiac images have the following codes in the UKB application:
1. 20208:Long axis image 
2. 20209:Short axis image
3. 20210:Aortic distensibility image
4. 20211:Cine image
5. 20212:LVOT image

To download these images, first we need to identify the EIDs of participants who have these images taken. Take a look at 
https://biobank.ctsu.ox.ac.uk/~bbdatan/Accessing_UKB_data_v2.3.pdf on instructions to access the UKB data. 
First find the UKB participants showcase file and then identify columnlist with EIDs using the following command 
(this looks for all cases that have 20208 imaging done):
```
head -n1 /lustre/projects/ICS_UKB/processed_phenotypes/merges/latest.txt | tr $'\t' '\n' | cat -n | grep "20208"
```
Then use the following command to cut columns that are of interest. In the following case, line 182-188 was identified
as 20208 to 20214 that are all imaging codes:
```
cut -f1,182-188 /lustre/projects/ICS_UKB/processed_phenotypes/merges/latest.txt > column_extract.txt
```
This file will contain the EIDs and the Imaging codes as the columns. Next step is to extract a file that contains EIDs
followed by specific imaging code as follows from the previously generated file, where left column is EID and right 
column is the specific image that needs to be downloaded. Rename this file as *bulkIDs_20208.csv* for this example.

```
1805024 20208_2_0
1432680 20208_2_0
5961785 20208_2_0
1275156 20208_2_0
1722863 20208_2_0
3156196 20208_2_0
4227774 20208_2_0
3368854 20208_2_0
3945926 20208_2_0
2594206 20208_2_0
```

To download the bulk images contained in the *bulkIDs_20208.csv*, run the following command. Note that you need to have 
the key file from UKB (received via email from UKB) and other ukb utility scripts in the same folder:

```
./ukbfetch -bbulkIDs_20208.csv -s28001 -m1000 -ak12113x41791.key
```
**Note**: You can only download a maximum of 1000 images using the above-command. So you might need to script this in 
a bash script as follows to download on myriad. This script will download first 10k images of 20209 code and move them 
in a folder named *zip_files*:

```
#!/bin/bash -l

# Request ten minutes of wallclock time (format hours:minutes:seconds).
#$ -l h_rt=47:00:0

# Request 12 cores, 2 GPUs, 1 gigabyte of RAM per CPU, 15 gigabyte of TMPDIR space
#$ -l mem=1G
#$ -pe mpi 8
#$ -l tmpfs=10G
#$ -cwd
#$ -m be


# Set the name of the job.
#$ -N UKBfetch_20209

# Run our MPI job. You can choose OpenMPI or IntelMPI for GCC.
module load python 
module load tensorflow
export PYTHONPATH="${PYTHONPATH}:/home/rmgppal"
export PYTHONPATH="${PYTHONPATH}:/home/rmgppal/ukbb_cardiac_iimog/ukbb_cardiac"
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/rmgppal/Scratch/mirtk/lib
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/rmgppal/vtk/lib
module load beta-modules
module load cuda/11.3.1/gnu-10.2.0
module load cudnn/8.2.1.32/cuda-11.3

./ukbfetch -bbulkIDs_20209.csv -s1 -m1000 -ak12113x41791.key
./ukbfetch -bbulkIDs_20209.csv -s1001 -m1000 -ak12113x41791.key 
./ukbfetch -bbulkIDs_20209.csv -s2001 -m1000 -ak12113x41791.key
./ukbfetch -bbulkIDs_20209.csv -s3001 -m1000 -ak12113x41791.key
./ukbfetch -bbulkIDs_20209.csv -s4001 -m1000 -ak12113x41791.key
./ukbfetch -bbulkIDs_20209.csv -s5001 -m1000 -ak12113x41791.key
./ukbfetch -bbulkIDs_20209.csv -s6001 -m1000 -ak12113x41791.key
./ukbfetch -bbulkIDs_20209.csv -s7001 -m1000 -ak12113x41791.key
./ukbfetch -bbulkIDs_20209.csv -s8001 -m1000 -ak12113x41791.key
./ukbfetch -bbulkIDs_20209.csv -s9001 -m1000 -ak12113x41791.key

mv *.zip ./zip_files
```

##Converting images from ZIP to NIFTI format
The zip files now need to be converted to NiFTi imaging format. To do that, we use a code called 
*convertZipToNIFTI.py* which again needs to run on bash script submitted job on myriad. It takes a 
while to convert the files; use the following bash script:

```
#!/bin/bash -l

#$ -l h_rt=47:00:0
#$ -l mem=1G
#$ -pe mpi 8
#$ -l tmpfs=10G
#$ -cwd
#$ -m be

#$ -N NiftiConvert

module load python 
module load tensorflow
export PYTHONPATH="${PYTHONPATH}:/home/rmgppal"
export PYTHONPATH="${PYTHONPATH}:/home/rmgppal/ukbb_cardiac_iimog/ukbb_cardiac"
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/rmgppal/Scratch/mirtk/lib
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/rmgppal/vtk/lib
module load beta-modules
module load cuda/11.3.1/gnu-10.2.0
module load cudnn/8.2.1.32/cuda-11.3

python3 convertZipToNIFTI.py

```
The code *convertZipToNIFTI.py* can be found on: ``` /home/rmgppal/Scratch/ukb_images/UKBB_CMR_Floriaan_Application```
For each EIDs, a folder will be created with following files: 
```ao.nii.gz  la_2ch.nii.gz  la_3ch.nii.gz  la_4ch.nii.gz  sa.nii.gz```

## Calculating Cardiac Traits
Next, copy the *ukbb_cardiac* modified to work on myriad to evaluate the cardiac traits, move the
NifTi folders into the folder *demo_image* inside *ukbb_cardiac* and run the following bash command:

```
#!/bin/bash -l

#$ -l h_rt=47:00:0
#$ -l mem=1G
#$ -l gpu=1
#$ -pe mpi 8
#$ -l tmpfs=10G
#$ -cwd
#$ -m be

#$ -N CardiacTraitAnalysis

module load python 
module load tensorflow
export PYTHONPATH="${PYTHONPATH}:/home/rmgppal"
export PYTHONPATH="${PYTHONPATH}:/home/rmgppal/ukbb_cardiac_iimog/ukbb_cardiac"
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/rmgppal/Scratch/mirtk/lib
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/rmgppal/vtk/lib
module load beta-modules
module load cuda/11.3.1/gnu-10.2.0
module load cudnn/8.2.1.32/cuda-11.3

python3 demo_pipeline.py >> output.txt
```
Once run successfully, this should generate cardiac trait csv files inside *demo_csv* with each trait in a separate 
file.  

## References

[1] W. Bai, et al. Automated cardiovascular magnetic resonance image analysis with fully convolutional networks. 
Journal of Cardiovascular Magnetic Resonance, 20:65, 2018. [doi](https://doi.org/10.1186/s12968-018-0471-x)

[2] W. Bai, et al. Recurrent neural networks for aortic image sequence segmentation with sparse annotations. 
Medical Image Computing and Computer Assisted Intervention (MICCAI), 2018.
[doi](https://doi.org/10.1007/978-3-030-00937-3_67) 

[3] W. Bai, et al. A population-based phenome-wide association study of cardiac and aortic structure and function. 
Nature Medicine, 2020. [doi](https://www.nature.com/articles/s41591-020-1009-y)

[4] S. Petersen, et al. Reference ranges for cardiac structure and function using cardiovascular magnetic resonance
(CMR) in Caucasians from the UK Biobank population cohort. Journal of Cardiovascular Magnetic Resonance, 19:18, 2017.
[doi](https://doi.org/10.1186/s12968-017-0327-9)
