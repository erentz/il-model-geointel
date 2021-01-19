# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 12:02:44 2021

@author: malik
"""

import arcpy
import os
import pandas as pd
import datetime
import urllib
from arcpy import env

env.overwriteOutput     = True

print("Start time:")
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# try:
   # print(analysis_state)
# except:
   # analysis_state          =  input("What is the analysis state? - Don't include quotes in Python 3")
    
state_gdb               =   r'C:\USS\United States Solar Corporation\Site Selection - Documents\Data\State\IL\IL.gdb'# r'C:\USS\United States Solar Corporation\Site Selection - Documents\Data\State\{0}\{0}.gdb'.format(analysis_state)
env.workspace           =   state_gdb
phi                     =   "deletetemp"



permit_hurdles              = 'Political/IL_Permit_Hurdles'
