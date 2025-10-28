#!/bin/bash -l

#$ -l h_rt=47:00:0
#$ -l mem=1G
#$ -pe mpi 8
#$ -l tmpfs=10G
#$ -cwd
#$ -m be

#$ -N NiftiConvert

module purge
source /shared/ucl/apps/bin/defmods
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


