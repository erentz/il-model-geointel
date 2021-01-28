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
import pandas as pd
import numpy as np
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
cap_screen_res_dir      =   r'C:\USS\United States Solar Corporation\IL - State Level Resources\{}\Hosting Capacity\Capacity Screen Results'
phi                     =   'DeleteTemp'

cap_screen_table        =   "{}_CapacityScreen_Results"
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

def ExportDF2Arc(in_df, col_names, out_table):
    x = np.array(np.rec.fromrecords(in_df.values))
    x.dtype.names = tuple(col_names)
    if arcpy.Exists(os.path.join(env.workspace, out_table)):
        arcpy.Delete_management(os.path.join(env.workspace, out_table))
    arcpy.da.NumPyArrayToTable(x, os.path.join(env.workspace, out_table))

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

def UtilityMath(utility, in_df):
    if utility == 'ComEd':
      
        in_df["HostingCapacity"] = ((((in_df["Thermal_capacity_rating_of_feeder_or_sub"]+ in_df["_15pct_peak_load_kW"])-in_df["Existing_DER_on_feeder_or_sub"])/1)*.001)
 
    elif utility == 'Ameren':
   
       in_df["HostingCapacity"] = (((in_df["normal_rating_of_substation_MVA"]+ in_df["feeder_min_load_MVA"])-in_df["Existing_generation_on_circuit"])/1)
       
        
def ImportCapacityScreenData(in_utility):
    # Assign Utility Specific output
    points_fc               =   cap_screen_points_fc.format(util_dict[in_utility])
    cap_screen_results      =   cap_screen_table.format(util_dict[in_utility])
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
        arcpy.CalculateField_management(output_fc, "UID",'!Lat!+","+!Long!', 'PYTHON_9.3')
    # Merge all Cap Screens into one file
    arcpy.Merge_management(cap_screen_fc, points_fc)
    # Format Final Capacity Screen Request FC
    uss.delete_extra_fields(points_fc, ['UID', 'CapScreenName'])
    # Delete Intermediate
    for fc in cap_screen_fc:
        arcpy.Delete_management(fc)
        arcpy.Delete_management(fc+phi)
    #get capacity screen result file list    
    cap_screen_xlsx = []
    for file in os.listdir(cap_screen_res_dir.format(in_utility)):
        if file.endswith(".xlsx"):
            cap_screen_xlsx.append(os.path.join((cap_screen_res_dir.format(in_utility)), file))
   
    #import all capacity screen result files
    cap_screen_tables = []
    for xlsx in cap_screen_xlsx:
        xl = pd.ExcelFile(xlsx)
        #create DataFrame
        cap_screen_df = xl.parse(xl.sheet_names[0])
        # Table name
        out_table = in_utility + " " + os.path.basename(xlsx[:-5])
        out_table = out_table.replace(" ", "_") 
        ###Calculate hosting capacity based on utility specific variables
        UtilityMath(in_utility, cap_screen_df)
        #Create and assign Arc Compatible Column Names
        if in_utility == 'ComEd':
            ##make columns for Circuit & Substation which comed reports as same number
            cap_screen_df['Substation'] = cap_screen_df['feeder_or_sub_number']
            cap_screen_df['Circuit']    = cap_screen_df['feeder_or_sub_number']
            col_dict = {
            'CapScreenName': 'CapScreenName',
            'Substation' : 'Substation',
            'Circuit': 'Circuit',
            'feeder_or_sub_number': 'FeederName',
            'Thermal_capacity_rating_of_feeder_or_sub':'Substation_Cap_Rating_MVA',
            'Thermal_capacity_rating_of_feeder_or_sub':'Feeder_Cap_Rating_MVA',
            'Existing_DER_on_feeder_or_sub':'ExistingGenerationMVA',
            'Pending_DER_on_feeder_or_sub':'QueuedCapacityMW',
            'Available_capacity_on_feeder_or_sub':'Available_Sub_Cap_MW',
            'Available_capacity_on_feeder_or_sub':'Available_Feeder_Cap_MW',
            'Nominal_Voltage_at_sub': 'Distribution_Voltage_kv',
            'Obvious_Hurdles':'Obvious_Hurdle',
            'HostingCapacity':'HostingCapacity'
            }
            cap_screen_df.reindex(columns=col_dict)
            cap_screen_df = cap_screen_df[col_dict.keys()]
            cap_screen_df.rename(columns=col_dict, inplace=True) 
            column_list = list(cap_screen_df)
            ##convert units
            for field in column_list[4:-3]:
                cap_screen_df[field] = cap_screen_df[field]/(1000.0)
        else:
            ###Ameren Column Names
            col_dict = { 
            'CapScreenName': 'CapScreenName',   
            'Substation':'Substation',
            'POI_Ciruit': 'Circuit',
            'Feeder': 'FeederName',
            'normal_rating_of_substation_MVA':'Substation_Cap_Rating_MVA',
            'normal_rating_of_POI_circuit_MVA':'Feeder_Cap_Rating_MVA',
            'Existing_generation_on_circuit':'ExistingGenerationMVA',
            'queued_generation_mW':'QueuedCapacityMW',
            'available_substation_capacity_MVA':'Available_Sub_Cap_MW',
            'available_circuit_capacity_MVA':'Available_Feeder_Cap_MW',
            'substation_nominal_distribution_voltage':'Distribution_Voltage_kV',
            'Obvious_Hurdles':'Obvious_Hurdle',
            'HostingCapacity':'HostingCapacity'
            }
            cap_screen_df.reindex(columns=col_dict)
            cap_screen_df = cap_screen_df[col_dict.keys()]
            cap_screen_df.rename(columns=col_dict, inplace=True)
        column_list = list(cap_screen_df)
        cap_df = cap_screen_df[column_list]
        ExportDF2Arc(cap_df, column_list, out_table.format(util_dict[in_utility]))
        cap_screen_tables.append(out_table.format(util_dict[in_utility]))
    if arcpy.Exists(cap_screen_results):
        arcpy.Delete_management(cap_screen_results)
    arcpy.Merge_management(cap_screen_tables, cap_screen_results)
    fieldnames = []
    fields = arcpy.ListFields(cap_screen_results)
    for field in fields:
        if not field.required:
            fieldnames.append(field.name)
    fieldnames.remove( "CapScreenName")
    arcpy.JoinField_management(points_fc,"UID", cap_screen_results, "CapScreenName",fieldnames)
    
    for fc in cap_screen_fc:
        arcpy.Delete_management(fc)
        arcpy.Delete_management(fc+phi)
    for table in cap_screen_tables:
        arcpy.Delete_management(table)
    print("***ImportCapacityScreenData Finished!***")
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        

def Main():
    ImportSpatialUtilityData()
    for util in util_dict.keys():
        ImportCapacityScreenData(util)
    
try:
    drive
except:
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

Main()

print("End time:")
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
