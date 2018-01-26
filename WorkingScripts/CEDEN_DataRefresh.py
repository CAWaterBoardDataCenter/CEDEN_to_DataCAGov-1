'''

Author:
	Andrew Dix Hill; https://github.com/AndrewDixHill/CEDEN_to_DataCAGov ; andrew.hill@waterboards.ca.gov

Agency:
	California State Water Resource Control Board (SWRCB)
	Office of Information Management and Analysis (OIMA)

Purpose:
	This script is intended to query, clean and calculate new fields for
datasets from an internal SWRCB DataMart of CEDEN data. The original datasets contain
non-ascii characters and restricted character such as tabs, feedlines, return lines, etc which
this script removes. This script also applies a data quality estimate to every record.
This data quality estimate is calculated from a data quality decision tree found here XXXX.
	In addition, this script subsets the newly created datasets into much smaller and more
specialized data based on a list of analytes. Eventually this script will also publish each
dataset to the open data water portal on data.ca.gov although this step is currently non-
functional and under development.

How to use this script:
	From a powershell prompt (windows), call python and specify
	the complete path to this file. Below is an example, where XXXXXXX should be replaced
	with the filename and the path should be specific to the file location:
	python C:\\Users\\AHill\\Downloads\\XXXXXXX.py

Prerequisites:
	Windows platform (not strictly requirement but I was unable to get this library
		working on a mac... I tried)
	on mac
	Python 3.X
	pyodbc library for python.  See https://github.com/mkleehammer/pyodbc
	dkan library for python.    See https://github.com/GetDKAN/pydkan

'''

# Import the necessary libraries of python code
import pyodbc
import os, csv, re
from dkan.client import DatasetAPI
from datetime import datetime
##### These are not currently in use as we have decided not to calculate RB values for each site
#import time
#import json
#import pandas as pd
#import numpy as np
#import shapefile as shp
#from shapely.geometry import Polygon, Point

####   These lines are also not in use yet
#siteDictFile = "C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\WQX_Stations.txt"
#polygon_in = shp.Reader("C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\Regional_Board_Boundaries\\CA_WA_RBs_WGS84.shp")
#polygon = polygon_in.shapes()
#shpfilePoints = [shape.points for shape in polygon]
#polygons = shpfilePoints


#  This is the filter that every cell in each dataset gets passed through. From the "string" library, we are only
# allowing printable characters except pipes, quotes, tabs, returns, control breaks, etc.
import string
printable = set(string.printable)-set('|"\'`\t\r\n\f\v')


# Is this to be run for IR?
For_IR = False

# decodeAndStrip takes a string and filters each character through the printable variable. It returns a filtered string.
def decodeAndStrip(t):
	filter1 = ''.join(filter(lambda x: x in printable, str(t)))
	return filter1

###############################################################################
##################        Dictionaries for QA codes below 		###############
###############################################################################

# The following python dictionaries refer to codes and their corresponding data quality value as determined by
# Melissa Morris of SWRCB, Office of Information Management and Analysis. 0: QC record, 1: Passed QC, 2: Needs some
# review, 3: Spatial Accuracy Unknown, 4: Needs extensive review, 5: unknown data quality, 6: reject data record  (as
#  of 1/22/18)
QA_Code_list = {"AWM": 1, "AY": 2, "BB": 2, "BBM": 2, "BCQ": 1, "BE": 2, "BH": 1, "BLM": 4,
                "BRKA": 2, "BS": 2, "BT": 6, "BV": 4, "BX": 4, "BY": 4, "BZ": 4, "BZ15": 2,
                "C": 1, "CE": 4, "CIN": 2, "CJ": 2, "CNP": 2, "CQA": 1, "CS": 2, "CSG": 2,
                "CT": 2, "CVH": 1, "CVHB": 4, "CVL": 1, "CVLB": 4, "CZM": 2, "D": 1, "DB": 2,
                "DBLOD": 2, "DBM": 2, "DF": 2, "DG": 1, "DO": 1, "DRM": 2, "DS": 1, "DT": 1,
                "ERV": 4, "EUM": 4, "EX": 4, "F": 2, "FCL": 2, "FDC": 2, "FDI": 2, "FDO": 6,
                "FDP": 2, "FDR": 1, "FDS": 1, "FEU": 6, "FIA": 6, "FIB": 4, "FIF": 6, "FIO": 4,
                "FIP": 4, "FIT": 2, "FIV": 4, "FLV": 6, "FNM": 6, "FO": 2, "FS": 6, "FTD": 6,
                "FTT": 6, "FUD": 6, "FX": 4, "GB": 2, "GBC": 4, "GC": 1, "GCA": 1, "GD": 1,
                "GN": 4, "GR": 4, "H": 2, "H22": 4, "H25": 4, "H8": 2, "HB": 2, "HD": 4, "HH": 2,
                "HNO2": 2, "HR": 1, "HS": 4, "HT": 1, "IE": 2, "IF": 2, "IL": 4, "ILM": 2,
                "ILN": 2, "ILO": 2, "IM": 2, "IP": 4, "IP5": 4, "IPMDL2": 4, "IPMDL3": 4,
                "IPRL": 4, "IS": 4, "IU": 4, "IZM": 2, "J": 2, "JA": 2, "JDL": 2, "LB": 2,
                "LC": 4, "LRGN": 6, "LRIL": 6, "LRIP": 6, "LRIU": 6, "LRJ": 6, "LRJA": 6,
                "LRM": 6, "LRQ": 6, "LST": 6, "M": 2, "MAL": 1, "MN": 4, "N": 2, "NAS": 2,
                "NBC": 2, "NC": 1, "NG": 1, "NMDL": 1, "None": 1, "NR": 5, "NRL": 1, "NTR": 1,
                "OA": 2, "OV": 2, "P": 4, "PG": 4, "PI": 4, "PJ": 1, "PJM": 1, "PJN": 1, "PP": 4,
                "PRM": 4, "Q": 4, "QAX": 1, "QG": 4, "R": 6, "RE": 1, "REL": 1, "RIP": 6, "RIU": 6,
                "RJ": 6, "RLST": 6, "RPV": 4, "RQ": 2, "RU": 4, "RY": 4, "SC": 1, "SCR": 2,
                "SLM": 1, "TA": 4, "TAC": 1, "TC": 4, "TCI": 4, "TCT": 4, "TD": 4, "TH": 4,
                "THS": 4, "TK": 4, "TL": 2, "TNC": 2, "TNS": 1, "TOQ": 4, "TP": 4, "TR": 6, "TS": 4,
                "TW": 2, "UF": 2, "UJ": 2, "UKM": 4, "ULM": 4, "UOL": 2, "VCQ": 2, "VQN": 2, "VC": 2,
                "VBB": 2, "VBS": 2, "VBY": 4, "VBZ": 4, "VBZ15": 2, "VCJ": 2, "VCO": 2, "VCR": 2,
                "VD": 1, "VDO": 1, "VDS": 1, "VELB": 1, "VEUM": 4, "VFDP": 2, "VFIF": 6, "VFNM": 6,
                "VFO": 2, "VGB": 2, "VGBC": 4, "VGN": 4, "VH": 2, "VH25": 4, "VH8": 2, "VHB": 2,
                "VIE": 2, "VIL": 4, "VILN": 4, "VILO": 2, "VIP": 4, "VIP5": 4, "VIPMDL2": 4,
                "VIPMDL3": 4, "VIPRL": 4, "VIS": 4, "VIU": 4, "VJ": 2, "VJA": 2, "VLB": 2, "VLMQO": 2,
                "VM": 2, "VNBC": 2, "VNC": 1, "VNMDL": 1, "VNTR": 1, "VPJM": 1, "VPMQO": 2, "VQAX": 1,
                "VQCA": 4, "VQCP": 4, "VR": 6, "VRBS": 6, "VRBZ": 6, "VRDO": 6, "VRE": 1, "VREL": 1,
                "VRGN": 6, "VRIL": 6, "VRIP": 6, "VRIU": 6, "VRJ": 6, "VRLB": 6, "VRLST": 6, "VRQ": 2,
                "VRVQ": 6, "VS": 2, "VSC": 1, "VSCR": 2, "VSD3": 1, "VTAC": 1, "VTCI": 4, "VTCT": 4,
                "VTNC": 2, "VTOQ": 4, "VTR": 6, "VTW": 4, "VVQ": 6, "WOQ": 4, }

BatchVerificationCode_list = {"NA": 5, "NR": 5, "VAC": 1, "VAC:VCN": 6, "VAC:VMD": 2, "VAC:VMD:VQI": 4,
                              "VAC:VQI": 4, "VAC:VR": 6, "VAF": 1, "VAF:VMD": 2, "VAF:VQI": 4, "VAP": 1,
                              "VAP:VI": 4, "VAP:VQI": 4, "VCN": 6, "VLC": 1, "VLC:VMD": 2, "VLC:VMD:VQI": 4,
                              "VLC:VQI": 4, "VLF": 1, "VMD": 2, "VQI": 4, "VQI:VTC": 4, "VQN": 4, "VR": 6,
                              "VTC": 2}

ResultQualCode_list = {"/oC": 6, "<": 1, "<=": 1, "=": 1, ">": 1, ">=": 1, "A": 1, "CG": 6, "COL": 1, "DNQ": 1,
                       "JF": 1, "NA": 6, "ND": 1, "NR": 6, "NRS": 6, "NRT": 6, "NSI": 1, "P": 1, "PA": 1,
                       "w/C": 6, "": 1, }

TargetLatitude_list = {"-88": 6, "": 6, }
Result_list = {"": 1, }

StationCode_list = {"LABQA": 0, "LABQA_SWAMP": 0, "000NONPJ": 0, "FIELDQA": 0,
                    "Non Project QA Sample": 0, "Laboratory QA Sample": 0,
                    "Field QA sample": 0, }

SampleTypeCode_list = {"LabBlank": 0, "CompBLDup": 0, "LCS": 0, "CRM": 0,
                       "FieldBLDup_Grab": 0, "FieldBLDup_Int": 0, "FieldBLDup": 0,
                       "FieldBlank": 0, "TravelBlank": 0, "EquipBlank": 0, "DLBlank": 0,
                       "FilterBlank": 0, "MS1": 0, "MS2": 0, "MS3": 0, "MSBLDup": 0, }
ProgramName_list = {}
SampleDate_list = {"Jan  1 1950 12:00AM": 0, }
Analyte_list = {"Surrogate": 0, }
MatrixName_list = {"blankwater": 0,  "Blankwater": 0,  "labwater": 0,  "blankmatrix": 0, }
CollectionReplicate_list = {"0": 1, "1": 1, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0,  "7": 0,  "8": 0, }
ResultsReplicate_list = {"0": 1, "1": 1, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0,  "7": 0,  "8": 0, }

DQ_Codes = {0: "MetaData, QC record", 1: "Passed QC", 2: "Some review needed",
            3: "Spatial Accuracy Unknown", 4: "Extensive review needed",
            5: "Unknown data quality", 6: "Reject record", }

# the Codes_Dict variable is a dictionary template for each dataset. Some datasets do not have all of these columns
# and as such have to be removed with the DictionaryFixer definition below.
Codes_Dict = {"QACode": QA_Code_list,
              "BatchVerification": BatchVerificationCode_list,
              "ResultQualCode": ResultQualCode_list,
              "TargetLatitude": TargetLatitude_list,
              "Result": Result_list,
              "StationCode": StationCode_list,
              "SampleTypeCode": SampleTypeCode_list,
              "SampleDate": SampleDate_list,
              "ProgramName": ProgramName_list,
              "Analyte": Analyte_list,
              "MatrixName": MatrixName_list,
              "CollectionReplicate": CollectionReplicate_list,
              "ResultsReplicate": ResultsReplicate_list, }

#Unused
# siteLocations = {"StationCode": "", "StationName": "", }

# this is a Python dictionary of filenames and their Datamart names. This can be expanded by adding to the end of the
#  list. The FIRST key in this dictionary MUST be WQX_Stations. If For_IR is set to False, it will complete the
# normal weekly 5 tables. If For_IR is set to True, this script will complete the three IR tables.

if not For_IR:
	tables = {"WQX_Stations": "DM_WQX_Stations_MV", "WaterChemistryData": "WQDMart_MV",
          "ToxicityData": "ToxDmart_MV", "TissueData": "TissueDMart_MV",
          "BenthicData": "BenthicDMart_MV", "HabitatData": "HabitatDMart_MV", }
if For_IR:
	# Below is the line to run the IR tables.
	tables = {"WQX_Stations": "DM_WQX_Stations_MV", "IR_WaterChemistryData": "IR2018_WQ",
	          "IR_ToxicityData": "IR2018_Toxicity", "IR_BenthicData": "IR2018_Benthic",
	          "IR_STORET_2010": "IR2018_Storet_2010_2012", "IR_STORET_2012":
	          "IR2018_Storet_2012_2017", "IR_NWIS": "IR2018_NWIS", "IR_Field":
	          "IR2018_Field", "IR_TissueData": "IR2018_Tissue", }
	#tables = {"WQX_Stations": "DM_WQX_Stations_MV", "IR_TissueData": "IR2018_Tissue", }

###########################################################################################################################
#########################        Dictionaries for QA codes above		###############################################
###########################################################################################################################


###########################################################################################################################
#########################        Dictionary of code fixer 	below	###########################
###########################################################################################################################

# rename_Dict_Column simplifies the process of creating a new key in the dictionary and removing the old key.
def rename_Dict_Column(dictionary, oldName, Newname):
	dictionary[Newname] = dictionary[oldName]
	dictionary.pop(oldName)
	
def remove_Dict_Column(dictionary, removeName):
	dictionary.pop(removeName)

#The DictionaryFixer creates custom dictionaries for each dataset since not all columns are present or have the same
# name between datasets. Ex: Water chemistry has a "ProgramName" column while all of the other datasets use "Program"
# the Custom dictionary is called Codes_Dict_Alt and the ".pop" deletes unwanted keys. If you see a key error,
# check to see if your dataset has a column with that same name. If it is different, rename using rename_Dict_Column.
#  If it doesn't exist, use ".pop" to remove it as below.
def DictionaryFixer(Codes_Dict, filename ):
	Codes_Dict_Alt = Codes_Dict.copy()
	if filename == 'WQX_Stations':
		################################## Delete #################
		remove_Dict_Column(Codes_Dict_Alt, "Analyte")
		remove_Dict_Column(Codes_Dict_Alt, "Analyte")
		remove_Dict_Column(Codes_Dict_Alt, "Result")
		remove_Dict_Column(Codes_Dict_Alt, "MatrixName")
		remove_Dict_Column(Codes_Dict_Alt, "ResultsReplicate")
		remove_Dict_Column(Codes_Dict_Alt, "QACode")
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")
		remove_Dict_Column(Codes_Dict_Alt, "ResultQualCode")
		remove_Dict_Column(Codes_Dict_Alt, "TargetLatitude")
		remove_Dict_Column(Codes_Dict_Alt, "SampleTypeCode")
		remove_Dict_Column(Codes_Dict_Alt, "SampleDate")
		remove_Dict_Column(Codes_Dict_Alt, "ProgramName")
		remove_Dict_Column(Codes_Dict_Alt, "CollectionReplicate")

	if filename == 'BenthicData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="SampleTypeCode", Newname="SampleType")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		################################## Delete #################
		remove_Dict_Column(Codes_Dict_Alt, "Analyte")
		remove_Dict_Column(Codes_Dict_Alt, "Result")
		remove_Dict_Column(Codes_Dict_Alt, "MatrixName")
		remove_Dict_Column(Codes_Dict_Alt, "ResultsReplicate")
		remove_Dict_Column(Codes_Dict_Alt, "QACode")
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")

	if filename == 'TissueData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="MatrixName", Newname="Matrix")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultsReplicate", Newname="ResultReplicate")
		################################## Delete #################
		remove_Dict_Column(Codes_Dict_Alt, "ResultQualCode")

	if filename == 'WaterChemistryData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ProgramName", Newname="Program")

	if filename == 'ToxicityData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ProgramName", Newname="Program")
		rename_Dict_Column(Codes_Dict_Alt, oldName="BatchVerification", Newname="BatchVerificationCode")
		################################## Delete #################
		remove_Dict_Column(Codes_Dict_Alt, "ResultsReplicate")

	if filename == 'HabitatData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ProgramName", Newname="Program")
		################################## Delete #################
		remove_Dict_Column(Codes_Dict_Alt, "ResultsReplicate")
		remove_Dict_Column(Codes_Dict_Alt, "Result")
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")

	if filename == 'IR_ToxicityData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ProgramName", Newname="Program")
		#Batch verification exists but should not be used for IR as of 1/25/2018
		#rename_Dict_Column(Codes_Dict_Alt, oldName="BatchVerification", Newname="BatchVerificationCode")
		################################## Delete #################
		remove_Dict_Column(Codes_Dict_Alt, "ResultsReplicate")
		# Batch verification exists but should not be used for IR as of 1/25/2018
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")


	if filename == 'IR_BenthicData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		rename_Dict_Column(Codes_Dict_Alt, oldName="SampleTypeCode", Newname="SampleType")
		################################## Delete #################
		remove_Dict_Column(Codes_Dict_Alt, "Analyte")
		remove_Dict_Column(Codes_Dict_Alt, "Result")
		remove_Dict_Column(Codes_Dict_Alt, "MatrixName")
		remove_Dict_Column(Codes_Dict_Alt, "ResultsReplicate")
		remove_Dict_Column(Codes_Dict_Alt, "QACode")
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")

	if filename == 'IR_WaterChemistryData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		rename_Dict_Column(Codes_Dict_Alt, oldName="Analyte", Newname="AnalyteName")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultsReplicate", Newname="Replicate")
		################################## Delete #################
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")
		remove_Dict_Column(Codes_Dict_Alt, "CollectionReplicate")

	if filename == 'IR_STORET_2010':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="Analyte", Newname="AnalyteName")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultsReplicate", Newname="Replicate")
		################################## Delete #################
		#  Batch verification exists but should not be used for IR as of 1/25/2018
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")
		remove_Dict_Column(Codes_Dict_Alt, "CollectionReplicate")


	if filename == 'IR_STORET_2012':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="Analyte", Newname="AnalyteName")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultsReplicate", Newname="Replicate")
		################################## Delete #################
		#  Batch verification exists but should not be used for IR as of 1/25/2018
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")
		remove_Dict_Column(Codes_Dict_Alt, "CollectionReplicate")

	if filename == 'IR_NWIS':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="Analyte", Newname="AnalyteName")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultsReplicate", Newname="Replicate")
		################################## Delete #################
		#  Batch verification exists but should not be used for IR as of 1/25/2018
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")
		remove_Dict_Column(Codes_Dict_Alt, "CollectionReplicate")

	if filename == 'IR_Field':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultsReplicate", Newname="ResultReplicate")
		# IR_Field has Analyte and AnalyteName columns
		Codes_Dict_Alt["AnalyteName"] = Codes_Dict_Alt["Analyte"]
		################################## Delete #################
		#  Batch verification exists but should not be used for IR as of 1/25/2018
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")

	if filename == 'IR_TissueData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultsReplicate", Newname="ResultReplicate")
		rename_Dict_Column(Codes_Dict_Alt, oldName="MatrixName", Newname="Matrix")
		################################## Delete #################
		#  Batch verification exists but should not be used for IR as of 1/25/2018
		remove_Dict_Column(Codes_Dict_Alt, "BatchVerification")

	return Codes_Dict_Alt
###########################################################################################################################
#########################        Dictionary of code fixer 	above	###########################
###########################################################################################################################

# data_retrieval is the meat of this script. It takes the tables dictionary defined above, two dates (specified
# below), and a save location for the output files.
def data_retrieval(tables, StartYear, EndYear, saveLocation, sep, extension):
	# initialize writtenFiles where we will store the output complete file paths in list format.
	writtenFiles = []
	try:
		# a python cursor is a synonym to a recordset or resultset.
		# this is the connection to SWRCB internal DataMart. Server, IUD, PWD are set as environmental variables so
		# no passwords are in plain text, see "Main" below for importing examples. UID
		# below create a connection
		cnxn = pyodbc.connect(Driver='SQL Server Native Client 11.0', Server=SERVER1, uid=UID, pwd=PWD)
		# creates a cursor which will execute the sql statement
		cursor = cnxn.cursor()
	except:
		print("Couldn't connect to %s. It is down or you might have a typo somewhere. Make sure you've got the "
		      "right password and Server id. Check internet "
		      "connection." % SERVER1)
	# This loop iterates on every item in the tables list.
	for count, (filename, table) in enumerate(tables.items()):
		writtenFiles.append(os.path.join(saveLocation, '%s.%s' % (filename, extension)))
		##############################################################################
		########################## SQL Statement  ####################################
		##############################################################################
		# The DM_WQX_Stations_MV table should not be filtered by date but the significant difference between this
		# table and the others is that we are not calculating new fields a do not have to add columns. Also,
		# benthic dataset does not need the Spatial_Datum column
		if table == 'DM_WQX_Stations_MV':
			sql = "SELECT * FROM %s ;" % table
			cursor.execute(sql)
			columns = [desc[0] for desc in cursor.description]
		else:
			sql = "SELECT * FROM %s WHERE (SampleDate BETWEEN " % table + \
		      "CONVERT(datetime, '%d-01-01') " % StartYear + \
		      "AND CONVERT(datetime, '%d-12-31'));" % EndYear
			cursor.execute(sql)
			#  This is where we could change the order of all the columns.... don't forget to write them in the same
			# order
			if For_IR or filename == 'BenthicData':
				columns = [desc[0] for desc in cursor.description]\
				          + ['DataQuality'] + ['DataQualityIndicator']
			else:
				columns = [desc[0] for desc in cursor.description] \
				          + ['DataQuality'] + ['DataQualityIndicator'] \
				          + ['Spatial_Datum']
		##############################################################################
		########################## SQL Statement  ####################################
		##############################################################################
		#initialize Sitecolumns
		Sitecolumns = []
		# the First key in Tables must be the WQX_Stations. When count is 0, we do NOT read in
		#  the WQX stations. If the script has already processed past WQX_Stations (count>0) then we read in the file
		#  for accessing the datum associated with the station codes.

		if count > 0:
			with open(writtenFiles[0], 'r', newline='', encoding='utf8') as WQX_sites:
				WQX_Sites = {}
				SitesCounter = 0
				reader = csv.reader(WQX_sites, delimiter=sep, lineterminator='\n')
				for row in reader:
					if SitesCounter == 0:
						Sitecolumns = row
						SitesCounter += 1
					SiterowDict = dict(zip(Sitecolumns, row))
					WQX_Sites[SiterowDict['StationCode']] = SiterowDict['Datum']
		if 1 == 1:
			with open(writtenFiles[count], 'w', newline='', encoding='utf8') as csvfile:
				dw = csv.DictWriter(csvfile, fieldnames=columns, delimiter=sep, lineterminator='\n')
				dw.writeheader()
				writer = csv.writer(csvfile, csv.QUOTE_MINIMAL, delimiter=sep, lineterminator='\n')
				##########################
				if table == 'DM_WQX_Stations_MV':
					for row in cursor:
						filtered = [decodeAndStrip(t) for t in list(row)]
						newDict = dict(zip(columns, filtered))
						#point = Point(float(newDict["TargetLongitude"]), float(newDict["TargetLatitude"]))
						##########  this is so slow! and we've chosen not to use WB regions just yet########################
						#for count, polygon in enumerate(polygons):
						#	poly = Polygon(polygon)
						#	if count == 9 and newDict["CA_WB_Region"] != '':
						#		continue
						#	elif poly.contains(point):
								# print(polygon_in.records()[count][3])
						#		newDict["CA_WB_Region"] = polygon_in.records()[count][3]
						#	else:
						#		newDict["CA_WB_Region"] = 'Outside of SHP file'
						##########  this is so slow! and we've chosen not to use WB regions just yet########################
						writer.writerow(list(newDict.values()))
				else:
					for row in cursor:
						filtered = [decodeAndStrip(t) for t in list(row)]
						if For_IR or filename == 'BenthicData':
							newDict = dict(zip(columns, filtered + [''] + ['']))
						else:
							newDict = dict(zip(columns, filtered + [''] + [''] + ['']))
						DQ = []
						QInd = ''
						Codes_Dict_Alt = DictionaryFixer(Codes_Dict, filename)
						for codeCol in list(Codes_Dict_Alt):
							if codeCol == 'QACode':
								for codeVal in newDict[codeCol].split(','):
									if codeVal in list(Codes_Dict_Alt[codeCol]):
										DQ += [Codes_Dict_Alt[codeCol][codeVal]]
							elif codeCol == 'Analyte' or codeCol == 'AnalyteName':
								if bool(re.search('[Ss]urrogate', newDict[codeCol])):
									DQ += [0]
							elif codeCol == 'ResultQualCode' or codeCol == 'ResQualCode':
								for codeVal in [newDict[codeCol]]:
									if table == 'IR2018_WQ' or table == 'IR2018_Tissue' and codeVal == 'DNQ':
										yearTest = int(newDict['SampleDate'][-4:])
										if isinstance(yearTest, int) and yearTest < 2008:
											print("This year was changed: %d" % yearTest)
											DQ += [6]
										continue
									elif codeVal == 'DNQ' and int(newDict['SampleDate'][:4]) < 2008:
										DQ += [6]
										continue
									elif codeVal == 'ND':
										# the Benthic dataset does not have a result column
										# we therefore treat the exception of a ND value
										# as a passed record.
										try:
											RQC = newDict['Result']
											if not isinstance(RQC, str) and RQC > 0:
												DQ += [6]
											else:
												DQ += [1]
										except KeyError:
											DQ += [1]
									elif codeVal in list(Codes_Dict_Alt[codeCol]):
										DQ += [Codes_Dict_Alt[codeCol][codeVal]]
										continue
							else:
								for codeVal in [newDict[codeCol]]:
									if codeVal in list(Codes_Dict_Alt[codeCol]):
										DQ += [Codes_Dict_Alt[codeCol][codeVal]]
						MaxDQ = max(DQ)
						for codeCol in list(Codes_Dict_Alt):
							if codeCol == 'QACode':
								for codeVal in newDict[codeCol].split(','):
									if codeVal in list(Codes_Dict_Alt[codeCol]):
										if any([Codes_Dict_Alt[codeCol][codeVal] == 0,
										        Codes_Dict_Alt[codeCol][codeVal] == 1]):
											continue
										elif MaxDQ == Codes_Dict_Alt[codeCol][codeVal]:
											if QInd == '':
												QInd += codeCol
											elif QInd == 'QACode':
												continue
											else:
												QInd += ', ' + codeCol
									continue
							else:
								for codeVal in [newDict[codeCol]]:
									if codeVal in list(Codes_Dict_Alt[codeCol]):
										if any([Codes_Dict_Alt[codeCol][codeVal] == 0,
													Codes_Dict_Alt[codeCol][codeVal] == 1]):
											continue
										elif MaxDQ == Codes_Dict_Alt[codeCol][codeVal]:
											if QInd == '':
												QInd += codeCol
											else:
												QInd += ", " + codeCol
										continue
						if min(DQ) == 0:
							newDict['DataQuality'] = DQ_Codes[0]
						else:
							newDict['DataQuality'] = DQ_Codes[MaxDQ]
							newDict['DataQualityIndicator'] = QInd
						if For_IR or filename == 'BenthicData':
							pass
						else:
							try:
								newDict['Spatial_Datum'] = WQX_Sites[newDict['StationCode']]
							except KeyError:
								newDict['Spatial_Datum'] = 'NR'
						##########  this is so slow!!!!!  ##################################################
						#for row in WQX_Sites:
						#	if newDict['StationCode'] in row[2]:
						#		newDict['DatumProjection'] = row[11]
								#newDict['CA_Regional_Board'] = row[16]
						#	else:
						#		newDict['DatumProjection'] = 'Unknown'
								#newDict['CA_Regional_Board'] = 'Unlisted'
						##########  this is so slow!!!!!  ##################################################
						writer.writerow(list(newDict.values()))
				print("Finished data retrieval for the %s table" % table)
	#cnxn.close()
	return writtenFiles, WQX_Sites#, site_Columns

####################################################################################
############################# Select By Analyte Subset #############################
####################################################################################
def selectByAnalyte(path, fileName, analytes, newFileName, field_filter, sep):
	file = os.path.join(path, fileName)
	fileOut = os.path.join(path, newFileName)
	columns = []
	with open(file, 'r', newline='', encoding='utf8') as txtfile:
		reader = csv.reader(txtfile, delimiter=sep, lineterminator='\n')
		with open(fileOut, 'w', newline='', encoding='utf8') as txtfileOut:
			writer = csv.writer(txtfileOut, csv.QUOTE_NONE, delimiter=sep, lineterminator='\n')
			count = 0
			for row in reader:
				if count == 0:
					columns = row
					writer.writerow(row)
					count += 1
					continue
				rowDict = dict(zip(columns, row))
				if rowDict[field_filter] in analytes:
					writer.writerow(row)
				####################################################################################
				############################# Select By Analyte Subset #############################
				####################################################################################


##############################################################################
########################## Main Statement  ###################################
##############################################################################

# Necessary variables imported from user's environmental variables.
if __name__ == "__main__":
	sep = '|'
	extension = 'txt'
	print('\n\n\n\n')
	SERVER1 = os.environ.get('SERVER1')
	UID = os.environ.get('UID')
	PWD = os.environ.get('PWD')
	StartYear = 1950
	EndYear = 2018
	saveLocation = "C:\\Users\\AHill\\Documents\\CEDEN_DataMart"
	startTime = datetime.now()
	FILES, WQX_Sites = data_retrieval(tables, StartYear, EndYear, saveLocation, sep=sep, extension=extension)
	print("\n\n\t\tCompleted data retrieval and processing\n\t\t\tfrom internal DataMart\n\n")
	totalTime = datetime.now() - startTime
	seconds = totalTime.seconds
	minutes = seconds // 60
	seconds = seconds - minutes * 60
	print("Data retrieval and processing took %d minutes and %d seconds" % (minutes, seconds))

	# saved datasets are likely:
	# FILES[0]: "WQX_Stations"
	# FILES[1]: "WaterChemistryData"
	# FILES[2]: "ToxicityData"
	# FILES[3]: "TissueData"
	# FILES[4]: "BenthicData"
	# FILES[5]: "HabitatData"
	# use FILES[X] to subset future datasets, as in example below...

	############## Subsets of datasets Safe To Swim
	print("\nStarting data subset for Safe to Swim...")
	if not For_IR:
		WaterChem = FILES[1]
		path, fileName = os.path.split(WaterChem)
		analytes = ['E. Coli', 'Enterococcus', 'Coliform, Total', 'Coliform, Fecal', ]
		newFileName = 'SafeToSwim.csv'
		column_filter = 'Analyte'
		selectByAnalyte(path=path, fileName=fileName, newFileName=newFileName, analytes=analytes,
		                field_filter=column_filter, sep=sep)
		print("\t\tFinished writing data subset for Safe to Swim\n\n")
		############## Subsets of datasets Safe To Swim


		############## Subsets of datasets for Pesticides
		print("\nStarting data subset for Pesticides....")
		analytes = ["Acetamiprid", "Acibenzolar-S-methyl", "Aldicarb", "Aldicarb ", "Aldicarb Sulfone",
		            "Aldicarb Sulfoxide", "Aldrin", "Aldrin, Particulate", "Allethrin", "Ametryn", "Aminocarb", "AMPA",
		            "Anilazine", "Aspon", "Atraton", "Atrazine", "Azinphos Ethyl", "Azinphos Methyl", "Azoxystrobin",
		            "Barban", "Bendiocarb", "Benfluralin", "Benomyl", "Bensulfuron Methyl", "Bentazon", "Bifenox",
		            "Bifenthrin", "Bispyribac Sodium", "Bolstar", "Bromacil", "Captafol", "Captan", "Carbaryl",
		            "Carbendazim", "Carbofuran", "Carbophenothion", "Carfentrazone Ethyl", "Chlorantraniliprole",
		            "Chlordane", "Chlordane, cis-", "Chlordane, cis-, Particulate", "Chlordane, Technical",
		            "Chlordane, trans-", "Chlordane, trans-, Particulate", "Chlordene, cis-", "Chlordene, trans-",
		            "Chlorfenapyr", "Chlorfenvinphos", "Chlorobenzilate", "Chlorothalonil", "Chlorpropham", "Chlorpyrifos",
		            "Chlorpyrifos Methyl", "Chlorpyrifos Methyl, Particulate", "Chlorpyrifos Methyl/Fenchlorphos",
		            "Chlorpyrifos, Particulate", "Cinerin-2", "Ciodrin", "Clomazone", "Clothianidin", "Coumaphos",
		            "Cyanazine", "Cyantraniliprole", "Cycloate", "Cyfluthrin", "Cyfluthrin, beta-", "Cyfluthrin-1",
		            "Cyfluthrin-2", "Cyfluthrin-3", "Cyfluthrin-4", "Cyhalofop-butyl", "Cyhalothrin", "Cyhalothrin lambda-",
		            "Cyhalothrin, gamma-", "Cyhalothrin, lambda-1", "Cyhalothrin, lambda-2", "Cypermethrin",
		            "Cypermethrin-1", "Cypermethrin-2", "Cypermethrin-3", "Cypermethrin-4", "Cyprodinil", "Dacthal",
		            "Dacthal, Particulate", "DCBP(p,p')", "DDD(o,p')", "DDD(o,p'), Particulate", "DDD(p,p')",
		            "DDD(p,p'), Particulate", "DDE(o,p')", "DDE(o,p'), Particulate", "DDE(p,p')", "DDE(p,p'), Particulate",
		            "DDMU(p,p')", "DDMU(p,p'), Particulate", "DDT(o,p')", "DDT(o,p'), Particulate", "DDT(p,p')",
		            "DDT(p,p'), Particulate", "Deltamethrin", "Deltamethrin/Tralomethrin", "Demeton", "Demeton-O",
		            "Demeton-s", "Desethyl-Atrazine", "Desisopropyl-Atrazine", "Diazinon", "Diazinon, Particulate",
		            "Dichlofenthion", "Dichlone", "Dichloroaniline, 3,5-", "Dichlorobenzenamine, 3,4-",
		            "Dichlorophenyl Urea, 3,4-", "Dichlorophenyl-3-methyl Urea, 3,4-", "Dichlorvos", "Dichrotophos",
		            "Dicofol", "Dicrotophos", "Dieldrin", "Dieldrin, Particulate", "Diflubenzuron", "Dimethoate",
		            "Dioxathion", "Diphenamid", "Diphenylamine", "Diquat", "Disulfoton", "Dithiopyr", "Diuron",
		            "Endosulfan I", "Endosulfan I, Particulate", "Endosulfan II", "Endosulfan II, Particulate",
		            "Endosulfan Sulfate", "Endosulfan Sulfate, Particulate", "Endrin", "Endrin Aldehyde", "Endrin Ketone",
		            "Endrin, Particulate", "EPN", "EPTC", "Esfenvalerate", "Esfenvalerate/Fenvalerate",
		            "Esfenvalerate/Fenvalerate-1", "Esfenvalerate/Fenvalerate-2", "Ethafluralin", "Ethion", "Ethoprop",
		            "Famphur", "Fenamiphos", "Fenchlorphos", "Fenhexamid", "Fenitrothion", "Fenpropathrin", "Fensulfothion",
		            "Fenthion", "Fenuron", "Fenvalerate", "Fipronil", "Fipronil Amide", "Fipronil Desulfinyl",
		            "Fipronil Desulfinyl Amide", "Fipronil Sulfide", "Fipronil Sulfone", "Flonicamid", "Fluometuron",
		            "Fluridone", "Flusilazole", "Fluvalinate", "Fluxapyroxad", "Folpet", "Fonofos", "Glyphosate",
		            "Halosulfuron Methyl", "HCH, alpha-", "HCH, alpha-, Particulate", "HCH, beta-",
		            "HCH, beta-, Particulate", "HCH, delta-", "HCH, delta-, Particulate", "HCH, gamma-",
		            "HCH, gamma-, Particulate", "Heptachlor", "Heptachlor Epoxide", "Heptachlor Epoxide, Particulate",
		            "Heptachlor Epoxide/Oxychlordane", "Heptachlor Epoxide/Oxychlordane, Particulate",
		            "Heptachlor, Particulate", "Hexachlorobenzene", "Hexachlorobenzene, Particulate", "Hexazinone",
		            "Hydroxyatrazine, 2-", "Hydroxycarbofuran, 3- ", "Hydroxypropanal, 3-", "Imazalil", "Indoxacarb",
		            "Isofenphos", "Isoxaben", "Jasmolin-2", "Kepone", "Ketocarbofuran, 3-", "Leptophos", "Linuron",
		            "Malathion", "Merphos", "Methamidophos", "Methidathion", "Methiocarb", "Methomyl", "Methoprene",
		            "Methoxychlor", "Methoxychlor, Particulate", "Methoxyfenozide", "Methyl (3,4-dichlorophenyl)carbamate",
		            "Mevinphos", "Mexacarbate", "Mirex", "Mirex, Particulate", "Molinate", "Monocrotophos", "Monuron",
		            "Naled", "Neburon", "Nonachlor, cis-", "Nonachlor, cis-, Particulate", "Nonachlor, trans-",
		            "Nonachlor, trans-, Particulate", "Norflurazon", "Oxadiazon", "Oxadiazon, Particulate", "Oxamyl",
		            "Oxychlordane", "Oxychlordane, Particulate", "Oxyfluorfen", "Paraquat", "Parathion, Ethyl",
		            "Parathion, Methyl", "PCNB", "Pebulate", "Pendimethalin", "Penoxsulam", "Permethrin",
		            "Permethrin, cis-", "Permethrin, trans-", "Perthane", "Phenothrin", "Phorate", "Phosalone", "Phosmet",
		            "Phosphamidon", "Piperonyl Butoxide", "Pirimiphos Methyl", "PrAllethrin", "Procymidone", "Profenofos",
		            "Profluralin", "Prometon", "Prometryn", "Propachlor", "Propanil", "Propargite", "Propazine", "Propham",
		            "Propoxur", "Pymetrozin", "Pyrethrin-2", "Pyrimethanil", "Quinoxyfen", "Resmethrin", "Safrotin",
		            "Secbumeton", "Siduron", "Simazine", "Simetryn", "Sulfallate", "Sulfotep", "Tebuthiuron", "Tedion",
		            "Terbufos", "Terbuthylazine", "Terbutryn", "Tetrachloro-m-xylene", "Tetrachlorvinphos",
		            "Tetraethyl Pyrophosphate", "Tetramethrin", "T-Fluvalinate", "Thiamethoxam", "Thiobencarb", "Thionazin",
		            "Tokuthion", "Total DDDs", "Total DDEs", "Total DDTs", "Total HCHs", "Total Pyrethrins", "Toxaphene",
		            "Tralomethrin", "Tributyl Phosphorotrithioate, S,S,S-", "Trichlorfon", "Trichloronate", "Triclopyr",
		            "Tridimephon", "Vinclozolin", ]
		WaterChem = FILES[1]
		path, fileName = os.path.split(WaterChem)
		newFileName = 'Pesticides.csv'
		column_filter = 'DW_AnalyteName'
		selectByAnalyte(path=path, fileName=fileName, newFileName=newFileName,
		                analytes=analytes, field_filter=column_filter, sep=sep)
		print("\t\tFinished writing data subset for Pesticides\n\n")
		############## Subsets of datasets for Pesticides

	if For_IR:
		RB = list(range(1, 10)) + ['']
		for IR_file in FILES[1:]:
			for Region in RB:
				path, fileName = os.path.split(IR_file)
				file_parts = os.path.splitext(fileName)
				if file_parts[0] == 'IR_STORET_2010' or file_parts[0] == 'IR_STORET_2012' or file_parts[0] == 'IR_NWIS':
					continue
				# add a check and create folder called By_RB
				if Region == '':
					newFileName = 'By_RB\\' + file_parts[0] + '_RB_NR' + file_parts[1]
				else:
					newFileName = 'By_RB\\' + file_parts[0] + '_RB_' + str(Region) + file_parts[1]
				if file_parts[0] == 'IR_ToxicityData' or file_parts[0] == 'IR_Field':
					column_filter = 'RegionalBoard'
				else:
					column_filter = 'RegionalBoardID'
				analytes = [str(Region), ]
				selectByAnalyte(path=path, fileName=fileName, newFileName=newFileName, analytes=analytes, field_filter=column_filter, sep=sep)
				print('Completed %s' % newFileName)

#for testing purposes only should be deleted soon.
#FILES = ['C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\WQX_Stations.txt',
#         'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\IR_ToxicityData.txt',
#         'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\IR_Field.txt',
#         'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\IR_NWIS.txt',
#         'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\IR_STORET_2010.txt',
#         'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\IR_STORET_2012.txt',
#         'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\IR_BenthicData.txt',
#         'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\IR_TissueData.txt'
#         'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\IR_WaterChemistryData.txt', ]


		###################################################################################################
#################################        Push data to data.ca.gov 		############################################
###################################################################################################
# NODE Testing
# 1912 Data Update Automation, Large File Loading
# 2061 Testing

# 1906 CEDEN Chemistry Data
#  541 Surface Water Toxicity
#  431 CEDEN Benthic Data
#  451 CEDEN Tissue Data
# 2011 CEDEN Habitat Data
# 1911 CEDEN Safe to Swim



NODE = 2061


#fileWritten = FILES[1]

user = os.environ.get('DCG_user')
password = os.environ.get('DCG_pw')
URI = os.environ.get('URI')
fileWritten = 'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\testExtraSmall2.csv'
# Attach dataset data
#try:
	#Sign into the data.ca.gov website
	#print("Connecting to data.ca.gov")
#api = DatasetAPI(URI, user, password )
	#print("Connected")
	#print("Pushing data")
#r = api.attach_file_to_node(file=fileWritten, node_id=NODE, field='field_upload')
#except:
	#print('need this line')

# Good for inspecting features of dataset/resource.
	#r = api.node('retrieve', node_id=NODE)
	#print(r.json())
	# use following line to inspect full response
	# r.json()
	# r.json()['nid'] #returns value for nid only. Other fields can be returned in this manner.


#except:
#	print(r.json())
#	print("Try... try again")

#os.remove(fileWritten)

#cnxn.close()
#del cnxn


