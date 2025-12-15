import os
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.art3d import Poly3DCollection
from skimage import measure
from skimage.draw import ellipsoid
import scipy.ndimage, skimage
import pyvista as pv
from pyvista import examples
import pydicom
import scipy
import vtk
from vtk.util import numpy_support as nps
import vmtk_centerlines_open_profiles
import vtk
from vmtk import vmtksurfacereader
from vmtk import vmtkcenterlinegeometry as vmtkCG
from vmtk import vmtkcenterlinestonumpy
import pandas as pd
import csv
import re

# Marching cubes
def _numpy_to_vtk_triangle_polydata(points, faces):
    '''
    reconstruct a polydata object
    arguments:
        points (_, 3) float array: points coordinates
        faces  (_, 3) (or 4) uint array: face vertex indices, triangle only.
    return:
        pd polydata
     '''
    if points.shape[1] != 3:
        raise ValueError('_numpy_to_vtk_triangle_polydata: invalid point array')

    if (faces.shape[1] == 4) and np.all(faces[:,0] == 3):
        faces = faces[:,1:]
    elif (faces.shape[1] != 3):
        raise ValueError('_numpy_to_vtk_triangle_polydata: invalid face array')
    elif (faces.max() >= points.shape[0]):
        raise ValueError('_numpy_to_vtk_triangle_polydata: invalid face indices')
    else:
        pass

    pt = vtk.vtkPoints()
    pt.SetData(nps.numpy_to_vtk(points))

    cl = vtk.vtkCellArray()
    for i in range(faces.shape[0]):
        cl.InsertNextCell(3, tuple(faces[i,:]))

    pd = vtk.vtkPolyData()
    pd.SetPoints(pt)
    pd.SetPolys(cl)

    return pd

#only old version of mesh can be processed with vmtk and vtk==8.1.0 in vmtk environment
def save_polydata_as_vtk(polydata, filename):
    writer = vtk.vtkPolyDataWriter()
    writer.SetFileName(filename)
    writer.SetFileVersion(42)
    writer.SetInputData(polydata)
    writer.Write()

#Post-processing mesh (for UKB cases only, need to reverse the cropping before processing into meshes)
crop_list= pd.read_csv('cropUKB.csv') #this is the file saved the auto-cropping parameters of UKB images
mask_directory = 'UKBMasks/' # Directory containing masks
mask_files= os.listdir(mask_directory)
for mask in mask_files:
    raw=np.load(os.path.join(mask_directory, mask))
    print(raw.shape)
    print(mask[:7]) #mask[:7] is the type of str, so need Eid[i] to be str as well
    for i in range(len(crop_list)):
        if crop_list.Eid[i]==mask[:7]:
            padded1 = np.pad(raw, ((0, 0), (crop_list.Front[i], 208-crop_list.Back[i]), (crop_list.Left[i], 192-crop_list.Right[i])), mode='constant', constant_values=0) #reverse the autocrop
            print('Size after 1st pad:', padded1.shape)           
            padded2 = np.pad(padded1, ((0, 0), (12, 20), (24, 24)), mode='constant', constant_values=0) #reverse the fixed crop
            image_flipped = np.flip(padded2, axis=0)#need flip once
            print('Size after 2nd pad:', image_flipped.shape)
            output_path= os.path.join('UKBMaskProcessed/', mask)
            np.save(output_path, image_flipped)
            verts, faces, normals, values = measure.marching_cubes(image_flipped, level=0.5)
            poly=_numpy_to_vtk_triangle_polydata(verts, faces)
            save_polydata_as_vtk(poly, 'UKBmesh/'+mask[:7]+'.vtk')

scout_directory= 'UKB_scout_dicoms/' #have to sort the dicoms in correct order to use the correct info of IOP for translation
mesh_directory= 'UKBmesh/'
meshlist=os.listdir(mesh_directory)
for filename in meshlist:
    mesh = pv.read(os.path.join(mesh_directory, filename))
    raw_mesh=mesh.points #get the points of mesh
    rotate_mesh= raw_mesh[:, ::-1] #make x, y, z match with h, w, l
    print(filename[:7])
    dicom_folder= filename[:7]
    dicom_path=  os.path.join(scout_directory, dicom_folder)
    dicom_each= os.listdir(dicom_path)
    slice_positions = []     #dicom images need to be sorted by their slice position
    for dicom in dicom_each:       
        meta_image = pydicom.dcmread(os.path.join(dicom_path, dicom),force=True)       
        slice_position = meta_image.SliceLocation      
        slice_positions.append((slice_position, dicom))  
    sorted_slices = sorted(slice_positions, key=lambda x: x[0])  
    sorted_file_names = [file for _, file in sorted_slices]  
    thisdicom=pydicom.dcmread(os.path.join(dicom_path,sorted_file_names[-1]))  
    IOP=thisdicom.ImageOrientationPatient   
    ps=float(thisdicom.PixelSpacing[0]) 
    print(ps)  
    scale_mesh= rotate_mesh*ps #scale the mesh to mm in real   
    IPP=np.array(thisdicom.ImagePositionPatient)  
    print(IPP)   
    tran_parameter=IPP-np.array([0,0,144*ps])
    print(tran_parameter)
    real_mesh= scale_mesh+tran_parameter  
    new= pv.read(os.path.join(mesh_directory, filename))    
    new.points = real_mesh
    new.save('TrueMesh/'+filename[:7]+'.vtk') #mesh with true size in true position in real space

def normal_vector(plane):
    x, y, z, dx1, dy1, dz1, dx2, dy2, dz2 = plane 
    v1 = np.array([dx1, dy1, dz1])
    v2 = np.array([dx2, dy2, dz2])
    normal_vector = np.cross(v1, v2)
    return normal_vector

#Use the info from dicom of phase contrast images to clip the mesh
mesh_path= 'TrueMesh/'
mesh_list=os.listdir(mesh_path)
image_path = 'Phase_dicom/'
for filename in mesh_list:
   
    mesh = pv.read(os.path.join(mesh_path, filename))
    plane_path = image_path+filename[:7]
    print(plane_path)
    try:
        dicom_files = [os.path.join(plane_path, f) for f in os.listdir(plane_path) if f.endswith('.dcm')]
        ds = pydicom.dcmread(dicom_files[3]) #using info the 4th slice to clip
        image_position_patient=ds.ImagePositionPatient
        image_orientation_patient=ds.ImageOrientationPatient
        cut_plane = list(image_position_patient) + list(image_orientation_patient)# Combine the two lists into one
        print(image_position_patient)
        cut_normal = normal_vector(cut_plane)
        print(cut_normal)
        clipped_mesh= mesh.clip(normal=cut_normal, origin=np.array(image_position_patient), invert= False)

        writer = vtk.vtkPolyDataWriter()#vtk will be save in version 42 for centerline purpose
        writer.SetInputData(clipped_mesh)
        writer.SetFileVersion(42)
        output_path= 'OpenEndMesh/'+filename[:7]+'.vtk'
        writer.SetFileName(output_path)
        writer.Write()
    except Exception as e:
        print(f"Not able to find path{plane_path}: {e}")

#centerline extraction (path length measurement)
mesh_path= 'OpenEndMesh/'
centerline_path ='centerline/'
mesh_list=os.listdir(mesh_path)

for filename in mesh_list:
    input_path = os.path.join(mesh_path, filename)
    output_path= os.path.join(centerline_path, filename)
    try:  
        vmtk_centerlines_open_profiles.centerlines_single(input_path, output_path)
    except:
        print(input_path, "Centerline not extracted for this mesh!!")
        pass

def fill_small_holes_clean_mesh (mesh, filename):

    clean_mesh = mesh.clean()
    filled_mesh = clean_mesh.fill_holes(5) # fill in holes that has radius less than 5mm
    # Get the connectivity of the mesh, where each connected component is labeled
    connectivity_filter = filled_mesh.connectivity()
    # Extract the largest connected component
    largest_component = connectivity_filter.extract_largest()

    writer = vtk.vtkPolyDataWriter()# Create a legacy writer #vtk will be save in version 42 to use vmtk.centerline
    writer.SetInputData(largest_component)
    writer.SetFileVersion(42)
    
    output_path= 'fixedMesh/'+filename
    writer.SetFileName(output_path)
    writer.Write()

# Check centerline measurement results
# Need Manual Check the cases 1) where no centerlines are generated 2) centerline with abnormal length
centerline_path ='centerline/'
length_list=[]
c_list=os.listdir(centerline_path)
for filename in c_list:
    cline = os.path.join(centerline_path, filename)
    reader_centerline = vmtksurfacereader.vmtkSurfaceReader()
    reader_centerline.InputFileName = cline
    reader_centerline.Execute()
    cg = vmtkCG.vmtkCenterlineGeometry() # Create a vmtkCenterlineGeometry object
    cg.Centerlines = reader_centerline.Surface # Set the centerline and segmentations
    cg.Execute() # Perform the centerline geometry analysis
    type(cg)
    cent2np = vmtkcenterlinestonumpy.vmtkCenterlinesToNumpy() #Converting the centerline to numpy array
    cent2np.Centerlines = cg.Centerlines
    cent2np.Execute()
    length=cent2np.ArrayDict['CellData']['Length']
    length_list.append(length)
    print(filename+' the centerline length is', length)
