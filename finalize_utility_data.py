# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 11:56:37 2021

@author: malik
"""

import arcpy
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import datetime
import ussolar.esri as uss
import pandas as pd
import numpy as np
from arcpy import env


env.overwriteOutput     = True

print("Start time:")
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

state_gdb               =   r'C:\Users\malik\Documents\GitHub\il-model-geointel\IL.gdb'
date_string             =   format(datetime.datetime.now().strftime('%Y%m%d'), "1")
cap_screen_res_dir      =   r'C:\USS\United States Solar Corporation\IL - State Level Resources\{}\Hosting Capacity\Capacity Screen Results'
cap_screen_points_fc    =   "UtilityData/{0}_CapacityScreenPoints"
env.workspace           =   state_gdb

manual_subs_kmz_id      =   '1_5mtkFe4VSPmnPanPU0XbV935NDHvE2q'


phi                     =   'DeleteTemp'

county_fc               =   "Political/IL_Counties"
substations_fc          =   'UtilityData/IL_Substations'
util_service_fc         =   'UtilityData/IL_ServiceAreas'
cap_screen_points_fc    =   "UtilityData/{0}_CapacityScreenPoints"
hosting_cap_table       =   "IL_HostingCapacity_Analysis"
output_net_subs_kmz     =   os.path.join(os.getcwd(), "DistributionSubs.kmz")
output_subs_ID          =   '1qPkmFhu86H72hpRAI6IqvHUGFr2_fcoR'
symbologyLayer          =   os.path.join(os.getcwd(), "IL_Sub_layer.lyr")
util_dict               =   {'ComEd':'ComEd', 
                             'Ameren':'Ameren'}

###uss.FC_to_pandas(feature_class, fieldnames = None)
### uss.delete_extra_fields(substations_fc+"_Manual", ['Name', 'UTILITY', 'COUNTY_NAME'])
def ExportDF2Arc(in_df, col_names, out_table):
    x = np.array(np.rec.fromrecords(in_df.values))
    x.dtype.names = tuple(col_names)
    if arcpy.Exists(os.path.join(env.workspace, out_table)):
        arcpy.Delete_management(os.path.join(env.workspace, out_table))
    arcpy.da.NumPyArrayToTable(x, os.path.join(env.workspace, out_table))

def FC2Pandas(feature_class, fieldnames = None):
    if fieldnames == None:
        fieldnames = []
        fields = arcpy.ListFields(feature_class)                     
        for field in fields:
            field = str(field.name)
            if field != 'Shape' and field != 'OBJECTID':
                fieldnames.append(field)
    #print fieldnames
    try:
        return pd.DataFrame(
            arcpy.da.FeatureClassToNumPyArray(
                in_table=feature_class,
                field_names=fieldnames,
                skip_nulls=False
            )
        )
    except:
        rows = []
        cursor = arcpy.da.SearchCursor(feature_class, fieldnames)
        for row in cursor:
            rows.append(row)
        df = pd.DataFrame(rows, columns = fieldnames)
        return df   

def HostingCapacityAnalysis(utility):
    #
    # Import Capacity Screen Data
    cap_result_dfs = []
    for util in util_dict.keys():
         cap_screen_results = cap_screen_points_fc.format(util_dict[util])
         cap_req_df = FC2Pandas(cap_screen_results)
         cap_req_df = cap_req_df[['Substation', 'ExistingGenerationMVA', 'HostingCapacity','QueuedCapacityMW']]
         cap_req_df['UTILITY'] = util
         cap_result_dfs.append(cap_req_df)
    cap_req_df = cap_result_dfs[0]
    for df in cap_result_dfs[1:]:
        cap_req_df = pd.concat([cap_req_df, df])
    # Import Queue data
  #  queue_df = uss.FC_to_pandas(queue_table) 
    
    # Summarize Capacity Results
    df_cap_sum = cap_req_df.groupby(["Substation", "UTILITY"], as_index=False)['HostingCapacity'].min()
    df_cap_sum2 = cap_req_df.groupby(["Substation", "UTILITY"], as_index=False)['ExistingGenerationMVA'].max()
    df_cap_sum3 = cap_req_df.groupby(["Substation", "UTILITY"], as_index=False)['QueuedCapacityMW'].max()
    df_cap_sum = pd.merge(df_cap_sum, df_cap_sum2, how='left', on=["Substation"])
    df_cap_sum['HostingCapacity']    = df_cap_sum['HostingCapacity'].fillna(0)
    df_cap_sum['ExistingGenerationMVA']    = df_cap_sum['ExistingGenerationMVA'].fillna(0)
    df_cap_sum = pd.merge(df_cap_sum, df_cap_sum3, how='left', on=["Substation"])

    # Calculate Available MW
    df_cap_sum.to_excel('captest.xlsx')
    df_cap_sum['Tot_Avail_MW'] = df_cap_sum['HostingCapacity'] - df_cap_sum['QueuedCapacityMW'] - df_cap_sum['ExistingGenerationMVA']
    df_cap_sum['Tot_Avail_MW']    = df_cap_sum['Tot_Avail_MW'].fillna(-999)
    df_cap_sum    = df_cap_sum.fillna(0)
    # adjust columns
    df_cap_sum = df_cap_sum[["Substation","UTILITY","HostingCapacity", "ExistingGenerationMVA", "Tot_Avail_MW", "QueuedCapacityMW"]]
    # Create substation level dataframe
    df_cols = []
    for col in df_cap_sum.columns:
        df_cols.append(col)
    substation_grouby_dfs = []
    for col in df_cols:
        if col != 'Substation' and col != 'FeederName':
            df_cap_bysub = df_cap_sum.groupby(['Substation'], as_index=False)[col].max()
            substation_grouby_dfs.append(df_cap_bysub)
    df_cap_bysub = substation_grouby_dfs[0]
    for df in substation_grouby_dfs[1:]:
        df_cap_bysub =  df_cap_bysub.merge(df , on = 'Substation', how='outer')
    # Export to Arc
    cols = df_cap_bysub.columns.tolist()
    arc_cols = []
    for col in cols:
        arc_cols.append(col.replace(" ", "_"))
    ExportDF2Arc(df_cap_bysub, arc_cols, hosting_cap_table)
    print ("***HostingCapacityAnalysis Finished!***")
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))   

"""def FixSubstationNameField(in_df, in_equiv_csv):
    ##Any substations with naming join issues will be printed out in a dataframe. Check substation name in equivalency field and update manual KMZ as needed.
    sub_name_equiv =  pd.read_csv(in_equiv_csv)
    sub_name_equiv.columns = ["Substation", "Final Substation Name"]
    in_df = in_df.merge(sub_name_equiv, how = 'left', on ='Substation')
    out_df_error = in_df.loc[in_df['Final Substation Name'].isnull()]
    if len(out_df_error)>0:
        print( out_df_error[["Substation"]])
    in_df['Substation'] = in_df["Final Substation Name"]
    in_df = in_df.drop(['Final Substation Name'], axis=1)
    return in_df     
def FinalizeSubstationFC():
    df = FC2Pandas(hosting_cap_table)
#   df = FixSubstationNameField(df, )"""
def Main():
    for util in util_dict.keys():
        HostingCapacityAnalysis(util)
Main()
#arcpy.Select_analysis(substations_fc + "_Manual", substations_fc)
