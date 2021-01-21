# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 11:45:12 2021

@author: malik
"""

import arcpy
import os
import pandas as pd
import datetime
import urllib
from arcpy import env
import sys
from zipfile import ZipFile


env.overwriteOutput     = True

arcpy.CheckOutExtension("Spatial")

print("Start time:")
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

state_gdb               =   r'C:\USS\United States Solar Corporation\Site Selection - Documents\Data\State\IL\IL.gdb' #r'C:\Users\Erich Rentz\Downloads\VA Scratch\VA.gdb' #
date_string             =   format(datetime.datetime.now().strftime('%Y%m%d'), "1")

env.workspace           =   state_gdb
scratch_space           =   r"C:\Users\malik\Downloads\Scratch" #adjust as necessary to an empty directory, files are managed
phi                     =   "DeleteTemp"
spatial_ref             =   2790
contourInterval         =   10

in_wetlands             =  r'C:\USS\United States Solar Corporation\Site Selection - Documents\Data\State\IL\Source\IL_geodatabase_wetlands.gdb\IL_Wetlands'
in_flood                =  r'C:\USS\United States Solar Corporation\Site Selection - Documents\Data\State\IL\Source\NFHL_17_20201230.gdb\S_Fld_Haz_Ar'
lc_gdb_list             = []

ned_list_csv            = "ned_list.csv"


county_fc               =   'Political\IL_Counties'
wetlands_fc             =   'Physical\IL_Wetlands'
flood_modern_fc         =   'Physical\IL_FloodHazard_Modern'
flood_unsuit_fc         =   'Physical\IL_FloodHazard_Unsuitable'
suitable_landcover_fc   =   'Physical\IL_Suitable_LC'

county_list             =   ['Kankakee', 'Champaign', 'Tazewell', 'LaSalle', 'Grundy,' ,'Will']

def ImportPhysicalData():
    arcpy.Select_analysis(in_wetlands, wetlands_fc)
    arcpy.Select_analysis(in_flood, flood_modern_fc)

    print("***ImportPhysicalData Finished!***")
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) 
    
    
def CreateDerivedPhysicalData():
    # Subset Unsuitable Flood Areas
    arcpy.Select_analysis(flood_modern_fc, flood_unsuit_fc, "FLD_ZONE in ('A', 'AE', 'AH', 'AO', '0.2 PCT ANNUAL CHANCE FLOOD HAZARD', 'VE', 'OPEN WATER')")
    
    
#def SetupSuitableLandcover():
    

def CreateUnsuitableSlopes(in_dem, outUnsuitSlope):    
    arcpy.ProjectRaster_management(in_dem, "project", spatial_ref)
    x = arcpy.Describe("project").meanCellWidth
    y = arcpy.Describe("project").meanCellHeight
    cellSizeXY = "{} {}".format(x, y)
    #print(cellSizeXY)
    arcpy.Resample_management("project", "resample", cellSizeXY, "CUBIC") 
    arcpy.Delete_management("project")
    # Run slope generation
    slope_raster = arcpy.sa.Slope("resample", "PERCENT_RISE")
    outInt = arcpy.sa.Int(slope_raster)
    arcpy.Delete_management("resample")
    del slope_raster
    # Set parameters for raster Reclassification as a boolean
    max_slope = outInt.maximum
    myRemapRange = arcpy.sa.RemapRange([[0, 10, 0], [10, int(max_slope), 1]])
    ### Run reclassification
    outReclassRR = arcpy.sa.Reclassify(outInt, "Value", myRemapRange) #
    del outInt
    ### Query Bad Slopes  
    #arcpy.ProjectRaster_management(outInt, "project", spatial_ref)
    slope_unSuit = arcpy.sa.ExtractByAttributes(outReclassRR, 'Value = 1')
    del outReclassRR
    ## Convert to Vector
    arcpy.RasterToPolygon_conversion(slope_unSuit, outUnsuitSlope, raster_field = "Value")
    del slope_unSuit
    # Clean up geometry
    arcpy.RepairGeometry_management(outUnsuitSlope)

def create_contours(in_dem, outContour, contourInterval):
    arcpy.ProjectRaster_management(in_dem, "project", spatial_ref)
    # Create focal statistics for smoother lines
    FS_raster = arcpy.sa.FocalStatistics("project", arcpy.sa.NbrCircle(5, 'CELL'), "MEAN","NODATA")
    # Run contour generation
    arcpy.sa.Contour(FS_raster, outContour, contourInterval, 0)
    # Delete Intermediate
    arcpy.Delete_management("project")
    arcpy.Delete_management(FS_raster)

def DefineUnsuitableSlopes():
    # Get NED data    
    df = pd.read_csv(ned_list_csv)
    download_urls = list(df['downloadURL'])
    dem_files = []
    for download_url in download_urls:
        # Create output file name variables
        file_name = os.path.split(download_url)[1]
        # print(file_name),
        # print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        new_download = os.path.join(scratch_space, file_name)
        # Download data
        if not os.path.exists(new_download):
            if sys.version_info[0] == 2: 
                urllib.urlretrieve (download_url, new_download)
            else:
                urllib.request.urlretrieve (download_url, new_download)
        dem_files.append(new_download)
    
    # Create unsuitable slopes and contours
    unsuitable_slope_list = []
    contours_list = []
    for in_dem in dem_files:
        print(in_dem),
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        outUnsuitSlope = state_gdb + "\{0}_UnsuitableSlopes".format(os.path.split(in_dem)[1][:-4])
        ## Create Unsuitable Slope Files by region
        CreateUnsuitableSlopes(in_dem, outUnsuitSlope)
        unsuitable_slope_list.append(outUnsuitSlope)
        ## Create Contour Files by region
        outContour = "Physical\Contours_{0}".format(os.path.split(in_dem)[1][:-4]) 
        create_contours(in_dem, outContour, contourInterval)
        contours_list.append(outContour)
    
    # Subset Unsuitable slope data by county
    for county in county_list:
        merge_list = []
        delete_list = []
        arcpy.Select_analysis(county_fc, phi, "COUNTY_NAME = '{}'".format(county))
        for unsuit_slope in unsuitable_slope_list:
            arcpy.Intersect_analysis([phi, unsuit_slope], unsuit_slope + phi)
            print(int(arcpy.GetCount_management(unsuit_slope + phi).getOutput(0)))
            if int(arcpy.GetCount_management(unsuit_slope + phi).getOutput(0))  > 0 :
                merge_list.append(unsuit_slope + phi)
            delete_list.append(unsuit_slope + phi)
        arcpy.Merge_management(merge_list, state_gdb + "\Physical\{0}_UnsuitableSlopes".format(county.replace(" ", "")))
        for i in delete_list:
            arcpy.Delete_management(i)
            
    arcpy.Delete_management(phi)
 
def Main():
    ImportPhysicalData()
    CreateDerivedPhysicalData()
   # SetupSuitableLandcover()
    DefineUnsuitableSlopes()
    
Main()
