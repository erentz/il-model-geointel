# -*- coding: utf-8 -*-
"""
Created on Fri Dec 06 10:22:02 2019

@author: malik
"""
import arcpy
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import pandas as pd
import numpy as np
import datetime

from arcpy import env
cap_screen_empty_dir             =   r"C:\USS\United States Solar Corporation\IL - State Level Resources\{0}\Hosting Capacity\Capacity Screen Points"
cap_screen_emptyresult_dir       =   r"C:\USS\United States Solar Corporation\IL - State Level Resources\{0}\Hosting Capacity\Capacity Screen Results"
utility_list                     =   ['ComEd', 'Ameren']

managed_subs_id                  =   '1YnFkLhdQg2CXHz45exypruhHT37Wy9Jf'
production_sub_id                =   '1QeYfuTOABAmvHoqUeHIyAEVdRb4hO2Qy'

phi                              =   "DeleteTemp"
working_gdb                      =   r'C:\USS\United States Solar Corporation\Site Selection - Documents\Data\State\IL\Illinois20180313.gdb'
arcpy.env.workspace              =   working_gdb
env.overwriteOutput              =   True
cap_screen_results               =   "{0}_CapacityScreen_Results"
cap_screen_points_mt             =   "{0}_CapacityScreenPoints"
result_table_mt                  =   "{0}_CapPointResults"
arc_columns = [
        "Name",
        "Circuit",
        "Feeder",
        "Substation_Cap_Rating_MVA",
        "Feeder_Cap_Rating_MVA",
        "Existing_DG_MW",
        "Queue_MW",
        "Available_Sub_Cap_MW",
        "Available_Feeder_Cap_MW",
        "Distribution_Voltage_kV",
        "Obvious_Hurdle"]
fields_dict = {
    'ArcNames':[
        "CapScreenName",
        "Name",
        "Circuit",
        "Feeder",
        "Substation_Cap_Rating_MVA",
        "Feeder_Cap_Rating_MVA",
        "Existing_DG_MW",
        "Queue_MW",
        "Available_Sub_Cap_MW",
        "Available_Feeder_Cap_MW",
        "Distribution_Voltage_kV",
        "Obvious_Hurdle",
        "HostingCapacity"],
    'Ameren':[
        "CapScreenName",   
        "Substation",
        "POI_Ciruit",
        "Feeder",
        "normal_rating_of_substation_MVA",
        "normal_rating_of_POI_circuit_MVA",
        "Existing_generation_on_circuit",
        "queued_generation_mW",
        "available_substation_capacity_MVA",
        "available_circuit_capacity_MVA",
        "substation_nominal_distribution_voltage",
        "Obvious_Hurdles",
        "HostingCapacity"],
    'ComEd':[
            "CapScreenName",
            "feeder_or_sub_number",
            "feeder_or_sub_number",
            "feeder_or_sub_number",
            "Thermal_capacity_rating_of_feeder_or_sub",
            "Thermal_capacity_rating_of_feeder_or_sub",
            "Existing_DER_on_feeder_or_sub",
            "Pending_DER_on_feeder_or_sub",
            "Available_capacity_on_feeder_or_sub",
            "Available_capacity_on_feeder_or_sub",
            "Nominal_Voltage_at_sub",
            "Obvious_Hurdles",
            "HostingCapacity"
           ]}

def DeleteExtraFields(in_table, keepfields):
    fieldnames = []
    fields = arcpy.ListFields(in_table)                     
    for field in fields:
        if not field.required:                              
            fieldnames.append(field.name)
    for keepfield in keepfields:
        try:
            fieldnames.remove(keepfield)
        except:
            pass                        
    if len(fieldnames)>0:
        arcpy.DeleteField_management(in_table, fieldnames)   
        
def ExportDF2Arc(in_df, col_names, out_table):
    x = np.array(np.rec.fromrecords(in_df.values))
    x.dtype.names = tuple(col_names)
    if arcpy.Exists(os.path.join(env.workspace, out_table)):
        arcpy.Delete_management(os.path.join(env.workspace, out_table))
    arcpy.da.NumPyArrayToTable(x, os.path.join(env.workspace, out_table))

   
    
    return in_df
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

def UtilityMath(utility, in_df):
    if utility == 'ComEd':
      
        in_df["HostingCapacity"] = ((((in_df["Thermal_capacity_rating_of_feeder_or_sub"]+ in_df["_15pct_peak_load_kW"])-in_df["Existing_DER_on_feeder_or_sub"])/1)*.001)

 
    elif utility == 'Ameren':
   
       in_df["HostingCapacity"] = (((in_df["normal_rating_of_substation_MVA"]+ in_df["feeder_min_load_MVA"])-in_df["Existing_generation_on_circuit"])/1)
       
      
        
    print in_df["HostingCapacity"]
def ImportCapacityScreenData(utility):
    cap_screen_req_dir    = cap_screen_empty_dir.format(utility)
    cap_screen_result_dir = cap_screen_emptyresult_dir.format(utility)
    cap_screen_points_fc  = cap_screen_points_mt.format(utility)
    cap_screen_xlsx = []
    cap_screen_kmz = []
    
  
    for file in os.listdir(cap_screen_req_dir):
        if file.endswith(".kmz"):
            cap_screen_kmz.append(os.path.join(cap_screen_req_dir, file))
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
        arcpy.CopyFeatures_management(output_fc+ phi, output_fc)
        
    arcpy.Merge_management(cap_screen_fc, cap_screen_points_fc)
    # Format Final Capacity Screen Request FC
    arcpy.AddField_management(cap_screen_points_fc, "xcent", "DOUBLE")
    arcpy.AddField_management(cap_screen_points_fc, "ycent", "DOUBLE")
    arcpy.CalculateField_management(cap_screen_points_fc, "xcent", "!SHAPE.CENTROID.X!",'PYTHON_9.3')
    arcpy.CalculateField_management(cap_screen_points_fc, "ycent", "!SHAPE.CENTROID.Y!",'PYTHON_9.3')
    arcpy.AddField_management(cap_screen_points_fc, "CapScreenName", "TEXT")
    arcpy.CalculateField_management(cap_screen_points_fc, "CapScreenName", "str(!xcent!) + ','+ str(!ycent!)",'PYTHON_9.3')
    
    for file in os.listdir(cap_screen_result_dir):
        if file.endswith(".xlsx"):
            cap_screen_xlsx.append(os.path.join(cap_screen_result_dir, file))
    # Import All Capacity Screen Result Files
    cap_screen_tables = []
 
    for xlsx in cap_screen_xlsx:       
        xl      = pd.ExcelFile(xlsx)
    
        # Create DataFrame
        cap_screen_df = xl.parse(xl.sheet_names[0])
        # Table name
        out_table = utility + " "+ os.path.basename(xlsx[:-5])
        print out_table
        out_table = out_table.replace(" ", "_")
        print cap_screen_df
        UtilityMath(utility, cap_screen_df)
        column_names=  fields_dict[utility]
        print type(column_names)
        column_list = list(column_names)
        print column_names
       
        if utility == 'ComEd':
            print cap_screen_df 
          
            for field in column_list[5:-5]:
                print field

                cap_screen_df[field] = cap_screen_df[field]/(1000.0)
                
            for field in column_list[9:-3]:
                cap_screen_df[field] = cap_screen_df[field]/(1000.0)
         
        else:
            pass
        cap_df = cap_screen_df[column_list]
 
        
        column_names =  fields_dict["ArcNames"]
        cap_df.columns = column_names
        column_list = list(column_names)
       
  
        ExportDF2Arc(cap_df, column_list, out_table)
        cap_screen_tables.append(out_table)
    results_table = result_table_mt.format(utility)
 
    arcpy.Merge_management(cap_screen_tables, results_table)
    fieldnames = []
    fields = arcpy.ListFields(results_table)                     
    for field in fields:
        if not field.required:                              
            fieldnames.append(field.name)
   
    arcpy.JoinField_management(cap_screen_points_fc, "CapScreenName", results_table, "CapScreenName")
    if utility == 'ComEd':
        arcpy.CalculateField_management(cap_screen_points_fc, "Name", "!Circuit!", "PYTHON_9.3")
    DeleteExtraFields(cap_screen_points_fc, fieldnames)
    if arcpy.Exists(cap_screen_results):
        arcpy.Delete_management(cap_screen_results)
        
    for fc in cap_screen_fc:
        arcpy.Delete_management(fc)
        arcpy.Delete_management(fc+phi)
   
    print "***ImportCapacityScreenData Finished!***"
def Main():
    for utility in utility_list:
      
        
        ImportCapacityScreenData(utility)

        print utility + "is completed"
        
Main()
