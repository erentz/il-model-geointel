# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 11:37:25 2021

@author: malik
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Oct 21 12:32:59 2020
@author: Erich Rentz
"""

import arcpy
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import datetime
import ussolar.esri as uss
from arcpy import env


env.overwriteOutput     = True

print("Start time:")
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

state_gdb               =   r'C:\Users\malik\Documents\GitHub\il-model-geointel\IL.gdb'
date_string             =   format(datetime.datetime.now().strftime('%Y%m%d'), "1")

env.workspace           =   state_gdb

manual_subs_kmz_id      =   '1_5mtkFe4VSPmnPanPU0XbV935NDHvE2q'
il_iou                  =   r'C:\USS\United States Solar Corporation\Site Selection - Documents\Data\State\IL\Manual\IL_IOU\IL_IOU.shp'
il_non_iou              =   r'C:\USS\United States Solar Corporation\Site Selection - Documents\Data\State\IL\Manual\IL_NON_IOU\IL_NON_IOU.shp'
cap_screen_req_dir      =   r"C:\USS\United States Solar Corporation\IL - State Level Resources\{}\Hosting Capacity\Capacity Screen Points"

phi                     =   'DeleteTemp'

county_fc               =   "Political/IL_Counties"
substations_fc          =   'UtilityData/IL_Substations'
util_service_fc         =   'UtilityData/IL_ServiceAreas'
cap_screen_points_fc    =   "UtilityData/{0}_CapacityScreenPoints"

util_dict               =   {'ComEd': 'ComEd',
                             'Ameren': 'Ameren'}

def ImportKMZ(in_kmz, output_fc, fc_type):
    # Define Path Variables
    scratch_directory       =   arcpy.Describe(in_kmz).path
    scratch_gdb_name        =   arcpy.Describe(in_kmz).basename
    # Create GDB From KMZ
    arcpy.KMLToLayer_conversion(in_kmz, scratch_directory, scratch_gdb_name, 'NO_GROUNDOVERLAY')
    # Define Target FC
    raw_fc                  =   os.path.join(scratch_directory,scratch_gdb_name+".gdb", "Placemarks", fc_type)
    # Copy to Final FC and Remove Z Flag
    arcpy.env.outputZFlag = "Disabled"
    arcpy.CopyFeatures_management(raw_fc, output_fc)
    # Delete Intermediate
    arcpy.Delete_management(os.path.join(scratch_directory,scratch_gdb_name+".lyr"))
    arcpy.Delete_management(os.path.join(scratch_directory,scratch_gdb_name+".gdb"))

def ImportSpatialUtilityData():
    # Grab Manual Substations
    file_obj = drive.CreateFile({'id': manual_subs_kmz_id})
    file_obj.GetContentFile('Subs.kmz')
    wkd = os.path.dirname(os.path.realpath(__file__))
    ImportKMZ(os.path.join(wkd, 'Subs.kmz'), substations_fc+"_Manual", "Points")
    # Cleanup Manual Subs
    arcpy.AddField_management(substations_fc+"_Manual", "UTILITY", "TEXT")
    arcpy.AddField_management(substations_fc+"_Manual", "COUNTY_NAME", "TEXT")
    arcpy.CalculateField_management(substations_fc+"_Manual", "UTILITY", '!FolderPath!.split("/")[-2]', "PYTHON_9.3")
    arcpy.CalculateField_management(substations_fc+"_Manual", "COUNTY_NAME", '!FolderPath!.split("/")[-1]', "PYTHON_9.3")
    uss.delete_extra_fields(substations_fc+"_Manual", ['Name', 'UTILITY', 'COUNTY_NAME'])
    # Combine Service Territory Data
    arcpy.Select_analysis(il_iou, "iou"+phi)
    arcpy.AddField_management("iou"+phi, "TYPE", "TEXT")
    arcpy.CalculateField_management("iou"+phi, "TYPE", "'Investor Owned'","PYTHON_9.3")
    arcpy.Select_analysis(il_non_iou, "non_iou"+phi)
    arcpy.AddField_management("non_iou"+phi, "TYPE", "TEXT")
    arcpy.CalculateField_management("non_iou"+phi, "TYPE", "'Non-IOU'", "PYTHON_9.3")

    arcpy.Merge_management(["non_iou"+phi,"iou"+phi], "util_merge"+phi)
    arcpy.Dissolve_management(county_fc, "IL")
    arcpy.Intersect_analysis(["util_merge"+phi, "IL"], util_service_fc + phi)
    arcpy.Dissolve_management(util_service_fc + phi, util_service_fc, ["Utility", "TYPE"])
    # Delete Intermediate
    arcpy.Delete_management('Subs.kmz')
    arcpy.Delete_management('IL')
    arcpy.Delete_management("iou"+phi)
    arcpy.Delete_management(util_service_fc + phi)
    arcpy.Delete_management("util_merge"+phi)
    arcpy.Delete_management("non_iou"+phi)
    print("***ImportSpatialUtilityData Finished!***")
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


# ---> Replace using output of the create_preapps script
def ImportCapacityScreenData(in_utility):
    # Assign Utility Specific output
    points_fc               =   cap_screen_points_fc.format(util_dict[in_utility])
    # Put together a list of Cap Screen KMZs
    cap_screen_kmz          =   []
    for f in os.listdir(cap_screen_req_dir.format(util_dict[in_utility])):
        if f.endswith(".kmz"):
            cap_screen_kmz.append(os.path.join(cap_screen_req_dir.format(util_dict[in_utility]), f))
    # Import All Capacity Screen KMZ Files
    cap_screen_fc = []
    for kmz in cap_screen_kmz:
        # Create Variables For Import
        output_fc = os.path.basename(kmz[:-4])
        output_fc = output_fc.replace(" ", "_")
        cap_screen_fc.append(output_fc)
        # Run Conversion
        ImportKMZ(kmz, output_fc+ phi, "Points")
        # Format Output FC
        arcpy.CalculateField_management(output_fc+ phi, 'FolderPath', '!FolderPath!.split("/")[-1]', 'PYTHON_9.3')
        # Subset Request Points
        arcpy.Select_analysis(output_fc+ phi, output_fc , 'Name <> FolderPath')
        # Create Final Capacity Screen Request FC
        arcpy.AddField_management(output_fc, "CapScreenName", "TEXT")
        print(output_fc)
        arcpy.CalculateField_management(output_fc, "CapScreenName",'!NAME!+"-"+!FolderPath!', 'PYTHON_9.3')
        arcpy.AddField_management(output_fc, "UID", "TEXT")
        arcpy.AddField_management(output_fc, "X", "DOUBLE")
        arcpy.AddField_management(output_fc, "Y", "DOUBLE")
        arcpy.CalculateField_management(output_fc, "Y", "!shape.firstpoint.X!","PYTHON_9.3")
        arcpy.CalculateField_management(output_fc, "X", "!shape.firstpoint.Y!","PYTHON_9.3")
        arcpy.CalculateField_management(output_fc, "X", "round(!X!,4)", "PYTHON_9.3")
        arcpy.CalculateField_management(output_fc, "Y", "round(!Y!,4)", "PYTHON_9.3")
        arcpy.AddField_management(output_fc, "Lat", "STRING")
        arcpy.CalculateField_management(output_fc, "Lat", '!X!', "PYTHON_9.3")
        arcpy.AddField_management(output_fc, "Long", "STRING")
        arcpy.CalculateField_management(output_fc, "Long", '!Y!', "PYTHON_9.3")
        arcpy.CalculateField_management(output_fc, "UID",'!Lat!+", "+!Long!', 'PYTHON_9.3')
    # Merge all Cap Screens into one file
    arcpy.Merge_management(cap_screen_fc, points_fc)
    # Format Final Capacity Screen Request FC
    uss.delete_extra_fields(points_fc, ['UID', 'CapScreenName'])
    # Delete Intermediate
    for fc in cap_screen_fc:
        arcpy.Delete_management(fc)
        arcpy.Delete_management(fc+phi)

def Main():
    ImportSpatialUtilityData()
    ImportCapacityScreenData('ComEd')
    
    ImportCapacityScreenData('Ameren')
   
try:
    drive
except:
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

Main()

print("End time:")
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
