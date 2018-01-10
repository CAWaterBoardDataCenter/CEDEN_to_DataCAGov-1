'''

Purpose:



'''

import pyodbc
#import pandas as pd
#import numpy as np
import os, csv
#import time
#import json
from dkan.client import DatasetAPI
#import shapefile as shp
#from shapely.geometry import Polygon, Point

siteDictFile = "C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\WQX_Stations.txt"
#polygon_in = shp.Reader("C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\Regional_Board_Boundaries\\CA_WA_RBs_WGS84.shp")
#polygon = polygon_in.shapes()
#shpfilePoints = [shape.points for shape in polygon]
#polygons = shpfilePoints


import string
printable = set(string.printable)-set('|"\'`\t\r\n\f\v')

def decodeAndStrip(t):
	filter1 = ''.join(filter(lambda x: x in printable, str(t)))
	return filter1


###############################################################################
##################        Dictionaries for QA codes below 		###############
###############################################################################

#These are dictionaries that hold the code and the Tier level of the data...
QA_Code_list = {"AWM": 1,"AY": 2,"BB": 2,"BBM": 2,"BCQ": 1,"BE": 2,"BH": 1,"BLM": 3,
                "BRKA": 2,"BS": 2,"BT": 5,"BV": 3,"BX": 3,"BY": 3,"BZ": 3,"BZ15": 2,
                "C": 1,"CE": 3,"CIN": 2,"CJ": 2,"CNP": 2,"CQA": 1,"CS": 2,"CSG": 2,
                "CT": 2,"CVH": 1,"CVHB": 3,"CVL": 1,"CVLB": 3,"CZM": 2,"D": 1,"DB": 2,
                "DBLOD": 2,"DBM": 2,"DF": 2,"DG": 1,"DO": 1,"DRM": 2,"DS": 1,"DT": 1,
                "ERV": 3,"EUM": 3,"EX": 3,"F": 2,"FCL": 2,"FDC": 2,"FDI": 2,"FDO": 5,
                "FDP": 2,"FDR": 1,"FDS": 1,"FEU": 5,"FIA": 5,"FIB": 3,"FIF": 5,"FIO": 3,
                "FIP": 3,"FIT": 2,"FIV": 3,"FLV": 5,"FNM": 5,"FO": 2,"FS": 5,"FTD": 5,
                "FTT": 5,"FUD": 5,"FX": 3,"GB": 2,"GBC": 3,"GC": 1,"GCA": 1,"GD": 1,
                "GN": 3,"GR": 3,"H": 2,"H22": 3,"H25": 3,"H8": 2,"HB": 2,"HD": 3,"HH": 2,
                "HNO2": 2,"HR": 1,"HS": 3,"HT": 1,"IE": 2,"IF": 2,"IL": 3,"ILM": 2,
                "ILN": 2,"ILO": 2,"IM": 2,"IP": 3,"IP5": 3,"IPMDL2": 3,"IPMDL3": 3,
                "IPRL": 3,"IS": 3,"IU": 3,"IZM": 2,"J": 2,"JA": 2,"JDL": 2,"LB": 2,
                "LC": 3,"LRGN": 5,"LRIL": 5,"LRIP": 5,"LRIU": 5,"LRJ": 5,"LRJA": 5,
                "LRM": 5,"LRQ": 5,"LST": 5,"M": 2,"MAL": 1,"MN": 3,"N": 2,"NAS": 2,
                "NBC": 2,"NC": 1,"NG": 1,"NMDL": 1,"None": 1,"NR": 5,"NRL": 1,"NTR": 1,
                "OA": 2,"OV": 2,"P": 3,"PG": 3,"PI": 3,"PJ": 1,"PJM": 1,"PJN": 1,"PP": 3,
                "PRM": 3,"Q": 3,"QAX": 1,"QG": 3,"R": 5,"RE": 1,"REL": 1,"RIP": 5,"RIU": 5,
                "RJ": 5,"RLST": 5,"RPV": 3,"RQ": 2,"RU": 3,"RY": 3,"SC": 1,"SCR": 2,
                "SLM": 1,"TA": 3,"TAC": 1,"TC": 3,"TCI": 3,"TCT": 3,"TD": 3,"TH": 3,
                "THS": 3,"TK": 3,"TL": 2,"TNC": 2,"TNS": 1,"TOQ": 3,"TP": 3,"TR": 5,"TS": 3,
                "TW": 2,"UF": 2,"UJ": 2,"UKM": 3,"ULM": 3,"UOL": 2,"VCQ": 2,"VQN": 2,"VC": 2,
                "VBB": 2,"VBS": 2,"VBY": 3,"VBZ": 3,"VBZ15": 2,"VCJ": 2,"VCO": 2,"VCR": 2,
                "VD": 1,"VDO": 1,"VDS": 1,"VELB": 1,"VEUM": 3,"VFDP": 2,"VFIF": 5,"VFNM": 5,
                "VFO": 2,"VGB": 2,"VGBC": 3,"VGN": 3,"VH": 2,"VH25": 3,"VH8": 2,"VHB": 2,
                "VIE": 2,"VIL": 3,"VILN": 3,"VILO": 2,"VIP": 3,"VIP5": 3,"VIPMDL2": 3,
                "VIPMDL3": 3,"VIPRL": 3,"VIS": 3,"VIU": 3,"VJ": 2,"VJA": 2,"VLB": 2,"VLMQO": 2,
                "VM": 2, "VNBC": 2,"VNC": 1,"VNMDL": 1,"VNTR": 1,"VPJM": 1,"VPMQO": 2,"VQAX": 1,
                "VQCA": 3, "VQCP": 3,"VR": 5,"VRBS": 5,"VRBZ": 5,"VRDO": 5,"VRE": 1,"VREL": 1,
                "VRGN": 5, "VRIL": 5,"VRIP": 5,"VRIU": 5,"VRJ": 5,"VRLB": 5,"VRLST": 5,"VRQ": 2,
                "VRVQ": 5, "VS": 2,"VSC": 1,"VSCR": 2,"VSD3": 1,"VTAC": 1,"VTCI": 3,"VTCT": 3,
                "VTNC": 2, "VTOQ": 3, "VTR": 5, "VTW": 3, "VVQ": 5, "WOQ": 3}

BatchVerificationCode_list = {"NA": 4,"NR": 4,"VAC": 1,"VAC:VCN": 5,"VAC:VMD": 2,"VAC:VMD:VQI": 3,
                              "VAC:VQI": 3,"VAC:VR": 5,"VAF": 1,"VAF:VMD": 2,"VAF:VQI": 3,"VAP": 1,
                              "VAP:VI": 3,"VAP:VQI": 3,"VCN": 5,"VLC": 1,"VLC:VMD": 2,"VLC:VMD:VQI": 3,
                              "VLC:VQI": 3,"VLF": 1,"VMD": 2,"VQI": 3,"VQI:VTC": 3,"VQN": 3,"VR": 5,
                              "VTC": 2}

ResultQualCode_list = {"/oC": 5,"<": 1,"<=": 1,"=": 1,">": 1,">=": 1,"A": 1,"CG": 5,"COL": 1,"DNQ": 1,
                       "JF": 1,"NA": 5,"ND": 1,"NR": 5,"NRS": 5, "NRT": 5, "NSI": 1, "P": 1, "PA": 1,
                       "w/C": 5, "": 1,}

TargetLatitude_list = {"-88": 5, "": 5, }
Result_list = {"":5}

StationCode_list = {"LABQA": 0,"LABQA_SWAMP": 0,"000NONPJ": 0,"FIELDQA": 0,
                    "Non Project QA Sample": 0,"Laboratory QA Sample": 0,
                    "Field QA sample": 0}

SampleTypeCode_list = {"LabBlank": 0, "CompBLDup": 0, "LCS": 0, "CRM": 0,
                       "FieldBLDup_Grab": 0, "FieldBLDup_Int": 0, "FieldBLDup": 0,
                       "FieldBlank": 0, "TravelBlank": 0, "EquipBlank": 0, "DLBlank": 0,
                       "FilterBlank": 0, "MS1": 0, "MS2": 0, "MS3": 0, "MSBLDup": 0}
ProgramName_list = {}
SampleDate_list = {"Jan  1 1950 12:00AM": 0, }
Analyte_list = {"Surrogate":0, }
MatrixName_list = {"blankwater": 0,  "Blankwater": 0,  "labwater": 0,  "blankmatrix": 0, }
CollectionReplicate_list = {"0": 1, "1": 1, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0,  "7": 0,  "8": 0, }
ResultsReplicate_list = {"0": 1, "1": 1, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0,  "7": 0,  "8": 0, }

DQ_Codes = {0: 'MetaData, QC record', 1: "Passed QC", 2: "Some review needed",
            3: "Extensive review needed", 4: "Unknown data quality", 5: "Reject record", }

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

tables = {"WQX_Stations": "DM_WQX_Stations_MV", "WaterChemistryData": "WQDMart_MV",
          "ToxicityData": "ToxDmart_MV", "TissueData": "TissueDMart_MV",
          "BenthicData": "BenthicDMart_MV", "HabitatData": "HabitatDMart_MV", }

siteLocations = {"StationCode": "", "StationName": "", }

###########################################################################################################################
#########################        Dictionaries for QA codes above		###############################################
###########################################################################################################################


###########################################################################################################################
#########################        Dictionary of code fixer 	below	###########################
###########################################################################################################################
def rename_Dict_Column(dictionary, oldName, Newname):
	dictionary[Newname] = dictionary[oldName]
	dictionary.pop(oldName)

def DictionaryFixer(Codes_Dict, filename ):
	Codes_Dict_Alt = Codes_Dict.copy()
	if filename == 'WQX_Stations':
		################################## Delete #################
		Codes_Dict_Alt.pop("Analyte")
		Codes_Dict_Alt.pop("Result")
		Codes_Dict_Alt.pop("MatrixName")
		Codes_Dict_Alt.pop("ResultsReplicate")
		Codes_Dict_Alt.pop("QACode")
		Codes_Dict_Alt.pop("BatchVerification")
		Codes_Dict_Alt.pop("ResultQualCode")
		Codes_Dict_Alt.pop("TargetLatitude")
		Codes_Dict_Alt.pop("SampleTypeCode")
		Codes_Dict_Alt.pop("SampleDate")
		Codes_Dict_Alt.pop("ProgramName")
		Codes_Dict_Alt.pop("CollectionReplicate")

	if filename == 'BenthicData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="SampleTypeCode", Newname="SampleType")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultQualCode", Newname="ResQualCode")
		################################## Delete #################
		Codes_Dict_Alt.pop("Analyte")
		Codes_Dict_Alt.pop("Result")
		Codes_Dict_Alt.pop("MatrixName")
		Codes_Dict_Alt.pop("ResultsReplicate")
		Codes_Dict_Alt.pop("QACode")
		Codes_Dict_Alt.pop("BatchVerification")

	if filename == 'TissueData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="MatrixName", Newname="Matrix")
		rename_Dict_Column(Codes_Dict_Alt, oldName="ResultsReplicate", Newname="ResultReplicate")
		################################## Delete #################
		Codes_Dict_Alt.pop("ResultQualCode")

	if filename == 'WaterChemistryData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ProgramName", Newname="Program")

	if filename == 'ToxicityData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ProgramName", Newname="Program")
		rename_Dict_Column(Codes_Dict_Alt, oldName="BatchVerification", Newname="BatchVerificationCode")
		################################## Delete #################
		Codes_Dict_Alt.pop("ResultsReplicate")

	if filename == 'HabitatData':
		################# Rename #################
		rename_Dict_Column(Codes_Dict_Alt, oldName="ProgramName", Newname="Program")
		################################## Delete #################
		Codes_Dict_Alt.pop("ResultsReplicate")
		Codes_Dict_Alt.pop("Result")
		Codes_Dict_Alt.pop("BatchVerification")
	return Codes_Dict_Alt

def data_retrieval(tables, StartYear, EndYear, saveLocation):
	writtenFiles = []
	try:
		# a python cursor is a synonym to a recordset or resultset.
		cnxn = pyodbc.connect(Driver='SQL Server Native Client 11.0', Server=SERVER1, uid=UID, pwd=PWD)
		cursor = cnxn.cursor()
	except:
		print("Couldn't connect to %s. It is down or you might have had a typo. Check internet connection." % SERVER1)
	for count, (filename, table) in enumerate(tables.items()):
		writtenFiles.append(os.path.join(saveLocation, '%s.txt' % filename))
		##############################################################################
		########################## SQL Statement  ####################################
		##############################################################################
		if table == 'DM_WQX_Stations_MV':
			sql = "SELECT * FROM %s ;" % table
			cursor.execute(sql)
			columns = [desc[0] for desc in cursor.description]
		else:
			sql = "SELECT * FROM %s WHERE (SampleDate BETWEEN " % table + \
		      "CONVERT(datetime, '%d-01-01') " % StartYear + \
		      "AND CONVERT(datetime, '%d-12-31'));" % EndYear
			cursor.execute(sql)
			columns = [desc[0] for desc in cursor.description] + ['DataQuality'] + ['DataQualityIndicator'] + [
				'Spatial_Datum']
		##############################################################################
		########################## SQL Statement  ####################################
		##############################################################################
		Sitecolumns = []
		if count > 0:
			with open(writtenFiles[0], 'r', newline='', encoding='utf8') as WQX_sites:
				WQX_Sites = {}
				SitesCounter = 0
				reader = csv.reader(WQX_sites, delimiter='\t', lineterminator='\n')
				for row in reader:
					if SitesCounter == 0:
						Sitecolumns = row
						SitesCounter += 1
					SiterowDict = dict(zip(Sitecolumns, row))
					WQX_Sites[SiterowDict['StationCode']] = SiterowDict['Datum']
		if 1 == 1:
			with open(writtenFiles[count], 'w', newline='', encoding='utf8') as csvfile:
				dw = csv.DictWriter(csvfile, fieldnames=columns, delimiter='\t', lineterminator='\n')
				dw.writeheader()
				writer = csv.writer(csvfile, csv.QUOTE_MINIMAL, delimiter='\t', lineterminator='\n')
				##########################
				if table == 'DM_WQX_Stations_MV':
					for row in cursor:
						filtered = [decodeAndStrip(t) for t in list(row)]
						newDict = dict(zip(columns, filtered ))
						#point = Point(float(newDict["TargetLongitude"]), float(newDict["TargetLatitude"]))
						##########  this is so slow!!!!!  ##################################################
						#for count, polygon in enumerate(polygons):
						#	poly = Polygon(polygon)
						#	if count == 9 and newDict["CA_WB_Region"] != '':
						#		continue
						#	elif poly.contains(point):
								# print(polygon_in.records()[count][3])
						#		newDict["CA_WB_Region"] = polygon_in.records()[count][3]
						#	else:
						#		newDict["CA_WB_Region"] = 'Outside of SHP file'
						##########  this is so slow!!!!!  ##################################################
						writer.writerow(list(newDict.values()))
				else:
					for row in cursor:
						filtered = [decodeAndStrip(t) for t in list(row)]
						if filename == 'BenthicData':
							newDict = dict(zip(columns, filtered + [''] + [''] ))
						else:
							newDict = dict(zip(columns, filtered + [''] + [''] + [''] ))
						DQ = []
						QInd = ''
						Codes_Dict_Alt = DictionaryFixer(Codes_Dict, filename)
						for codeCol in list(Codes_Dict_Alt):
							if codeCol == 'QACode':
								for codeVal in newDict[codeCol].split(','):
									if codeVal in list(Codes_Dict_Alt[codeCol]):
										DQ += [Codes_Dict_Alt[codeCol][codeVal]]
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
						if filename != 'BenthicData':
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
	cnxn.close()
	return writtenFiles, WQX_Sites#, site_Columns


##############################################################################
########################## Main Statement  ###################################
##############################################################################

# Necessary variables imported from user's environmental variables.
if __name__ == "__main__":
	SERVER1 = os.environ.get('SERVER1')
	UID = os.environ.get('UID')
	PWD = os.environ.get('PWD')
	StartYear = 1950
	EndYear = 2018
	saveLocation = "C:\\Users\\AHill\\Documents\\CEDEN_DataMart"
	FILES, WQX_Sites = data_retrieval(tables, StartYear, EndYear, saveLocation)




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
fileWritten = 'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\test.txt'
# Attach dataset data
#try:
	#Sign into the data.ca.gov website
	#print("Connecting to data.ca.gov")
api = DatasetAPI(URI, user, password )
	#print("Connected")
	#print("Pushing data")
r = api.attach_file_to_node(file=fileWritten, node_id=NODE, field='field_upload' )
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

cnxn.close()
del cnxn


