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
from arcpy import env


env.overwriteOutput     = True

print("Start time:")
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

state_gdb               =   r'C:\USS\United States Solar Corporation\Site Selection - Documents\Data\State\IL\IL.gdb'
date_string             =   format(datetime.datetime.now().strftime('%Y%m%d'), "1")

env.workspace           =   state_gdb

manual_subs_kmz_id      =   '1_5mtkFe4VSPmnPanPU0XbV935NDHvE2q'

phi                     =   'DeleteTemp'

county_fc               =   "Political/IL_Counties"
substations_fc          =   'UtilityData/IL_Substations'
util_service_fc         =   'UtilityData/IL_ServiceAreas'
cap_screen_points_fc    =   "UtilityData/{0}_CapacityScreenPoints"

util_dict               =   {}


arcpy.Select_analysis(substations_fc + "_Manual", substations_fc)
