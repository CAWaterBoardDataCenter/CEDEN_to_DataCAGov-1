'''

Purpose:

Benthic Data 1994 - Present
Tissue Data  1950 - Present

'''

import pyodbc, argparse
#import pandas as pd
#import numpy as np
import os, csv, re
import time
#import json
from dkan.client import DatasetAPI

import string
printable = set(string.printable)-set('|"\'`')
rep = {"\t": "", "\r\n": "", "\r": "", "\n": "", "\f": "", "\v": "", }

def decodeAndStrip(t):
	filter1 = ''.join(filter(lambda x: x in printable, str(t)))
	#rep = dict((re.escape(k), v) for k, v in rep.items())
	filter2 = filter1.replace('\t', '').replace('\r\n','').replace('\r','').replace('\n','').replace('\f','').replace('\v','')
	return filter2


###########################################################################################################################
#########################        SQL generator 		###########################
###########################################################################################################################

def DictionaryFixer(Codes_Dict, filename ):
	Codes_Dict_Alt = Codes_Dict.copy()
	if filename == 'BenthicData':
		################# Rename #################
		Codes_Dict_Alt["SampleType"] = Codes_Dict_Alt["SampleTypeCode"]
		Codes_Dict_Alt.pop("SampleTypeCode")
		Codes_Dict_Alt["ResQualCode"] = Codes_Dict_Alt["ResultQualCode"]
		Codes_Dict_Alt.pop("ResultQualCode")
		################################## Delete #################
		Codes_Dict_Alt.pop("Analyte")
		Codes_Dict_Alt.pop("Result")
		Codes_Dict_Alt.pop("MatrixName")
		Codes_Dict_Alt.pop("ResultsReplicate")
		Codes_Dict_Alt.pop("QACode")
		Codes_Dict_Alt.pop("BatchVerification")
	if filename == 'TissueData':
		################# Rename #################
		Codes_Dict_Alt["Matrix"] = Codes_Dict_Alt["MatrixName"]
		Codes_Dict_Alt.pop("MatrixName")
		Codes_Dict_Alt["ResultReplicate"] = Codes_Dict_Alt["ResultsReplicate"]
		Codes_Dict_Alt.pop("ResultsReplicate")
		################################## Delete #################
		Codes_Dict_Alt.pop("ResultQualCode")
	if filename == 'WaterChemistryData':
		################# Rename #################
		Codes_Dict_Alt["Program"] = Codes_Dict_Alt["ProgramName"]
		Codes_Dict_Alt.pop("ProgramName")
	if filename == 'ToxicityData':
		################# Rename #################
		Codes_Dict_Alt["Program"] = Codes_Dict_Alt["ProgramName"]
		Codes_Dict_Alt.pop("ProgramName")
		Codes_Dict_Alt["BatchVerificationCode"] = Codes_Dict_Alt["BatchVerification"]
		Codes_Dict_Alt.pop("BatchVerification")
		################################## Delete #################
		Codes_Dict_Alt.pop("ResultsReplicate")
	if filename == 'HabitatData':
		################# Rename #################
		Codes_Dict_Alt["Program"] = Codes_Dict_Alt["ProgramName"]
		Codes_Dict_Alt.pop("ProgramName")
		################################## Delete #################
		Codes_Dict_Alt.pop("ResultsReplicate")
		Codes_Dict_Alt.pop("Result")
		Codes_Dict_Alt.pop("BatchVerification")
	if filename == 'IR_ToxicityData':
		################# Rename #################
		Codes_Dict_Alt["ResQualCode"] = Codes_Dict_Alt["ResultQualCode"]
		Codes_Dict_Alt.pop("ResultQualCode")
		Codes_Dict_Alt["Program"] = Codes_Dict_Alt["ProgramName"]
		Codes_Dict_Alt.pop("ProgramName")
		Codes_Dict_Alt["BatchVerificationCode"] = Codes_Dict_Alt["BatchVerification"]
		Codes_Dict_Alt.pop("BatchVerification")
		################################## Delete #################
		Codes_Dict_Alt.pop("ResultsReplicate")
	if filename == 'IR_BenthicData':
		################# Rename #################
		Codes_Dict_Alt["SampleType"] = Codes_Dict_Alt["SampleTypeCode"]
		Codes_Dict_Alt.pop("SampleTypeCode")
		Codes_Dict_Alt["ResQualCode"] = Codes_Dict_Alt["ResultQualCode"]
		Codes_Dict_Alt.pop("ResultQualCode")
		################################## Delete #################
		Codes_Dict_Alt.pop("Analyte")
		Codes_Dict_Alt.pop("Result")
		Codes_Dict_Alt.pop("MatrixName")
		Codes_Dict_Alt.pop("ResultsReplicate")
		Codes_Dict_Alt.pop("QACode")
		Codes_Dict_Alt.pop("BatchVerification")
	if filename == 'IR_WaterChemistryData':
		################# Rename #################
		Codes_Dict_Alt["ResQualCode"] = Codes_Dict_Alt["ResultQualCode"]
		Codes_Dict_Alt.pop("ResultQualCode")
		Codes_Dict_Alt["AnalyteName"] = Codes_Dict_Alt["Analyte"]
		Codes_Dict_Alt.pop("Analyte")
		Codes_Dict_Alt["Replicate"] = Codes_Dict_Alt["ResultsReplicate"]
		Codes_Dict_Alt.pop("ResultsReplicate")
		################################## Delete #################
		Codes_Dict_Alt.pop("BatchVerification")
		Codes_Dict_Alt.pop("CollectionReplicate")
	return Codes_Dict_Alt

def data_retrieval(tables, StartYear, EndYear, saveLocation):
	writtenFiles = []
	try:
		cnxn = pyodbc.connect(Driver='SQL Server Native Client 11.0', Server=SERVER1, uid=UID, pwd=PWD)
	except:
		print("Couldn't connect to %s. It is down or you might have had a typo. Check internet connection." % SERVER1)
	# a python cursor is a synonym to a recordset or resultset.
	cursor = cnxn.cursor()
	for count, (filename, table) in enumerate(tables.items()):
		#print(count, table, filename)
		##############################################################################
		########################## SQL Statement  ####################################
		##############################################################################
		sql = "SELECT * FROM %s WHERE (SampleDate BETWEEN " % table + \
		      "CONVERT(datetime, '%d-01-01') " % StartYear + \
		      "AND CONVERT(datetime, '%d-12-31'));" % EndYear
		##############################################################################
		########################## SQL Statement  ####################################
		##############################################################################

		cursor.execute(sql)
		writtenFiles.append(os.path.join(saveLocation,'%s_%d-%d.txt' % (filename,StartYear,EndYear)))
		columns = [desc[0] for desc in cursor.description] + ['DataQuality'] + ['DataQualityIndicator']
		####  remove the " if/else of the lines below to over write files
		if os.path.isfile(writtenFiles[count]):
			print("\nFile %s exists" % writtenFiles[count])
		else:
			with open(writtenFiles[count], 'w', newline='', encoding='utf8') as csvfile:
				dw = csv.DictWriter(csvfile, fieldnames=columns, delimiter='\t', lineterminator='\n')
				dw.writeheader()
				writer = csv.writer(csvfile, csv.QUOTE_MINIMAL, delimiter='\t', lineterminator='\n')
				for row in cursor:
					filtered = [decodeAndStrip(t) for t in list(row)]
					#print('\n', filtered, '\n', type(filtered), '\n', len(filtered), '\n\n')
					newDict = dict(zip(columns, filtered + [''] + ['']))
					#print(len(filtered)+2, " ? == ? ", len(columns))
					#if len(filtered)+2 != len(columns):
					#	print('not enough columns')
					#	with open(os.path.join(saveLocation, str(writtenFiles[count] + "_tooShortError.csv")),
					# "a") as error1:
					#		error1.write(filtered)
					#		error1.write('\n')
					#	continue
					DQ = []
					QInd = ''
					Codes_Dict_Alt = DictionaryFixer(Codes_Dict, filename)
					for codeCol in list(Codes_Dict_Alt):
						if codeCol == 'QACode':
							for codeVal in newDict[codeCol].split(','):
								if codeVal in list(Codes_Dict_Alt[codeCol]):
									DQ += [Codes_Dict_Alt[codeCol][codeVal]]
						elif codeCol != 'QACode':
							for codeVal in [newDict[codeCol]]:
								if codeVal in list(Codes_Dict_Alt[codeCol]):
									DQ += [Codes_Dict_Alt[codeCol][codeVal]]
					MaxDQ = max(DQ)
					for codeCol in list(Codes_Dict_Alt):
						if codeCol == 'QACode':
							for codeVal in newDict[codeCol].split(','):
								if codeVal in list(Codes_Dict_Alt[codeCol]):
									if any([Codes_Dict_Alt[codeCol][codeVal] == 0, Codes_Dict_Alt[codeCol][codeVal] == 1]):
										continue
									elif MaxDQ == Codes_Dict_Alt[codeCol][codeVal]:
										if QInd == '':
											QInd += codeCol
										elif QInd == 'QACode':
											continue
										else:
											QInd += ', ' + codeCol
								continue
						elif codeCol != 'QACode':
							for codeVal in [newDict[codeCol]]:
								if codeVal in list(Codes_Dict_Alt[codeCol]):
									if any([Codes_Dict_Alt[codeCol][codeVal] == 0, Codes_Dict_Alt[codeCol][codeVal] == 1]):
										continue
									elif MaxDQ == Codes_Dict_Alt[codeCol][codeVal]:
										if QInd == '':
											QInd += codeCol
										else:
											QInd += ", " + codeCol
									continue
					if min(DQ) == 0:
						newDict['DataQuality'] = 0
					else:
						newDict['DataQuality'] = MaxDQ
						newDict['DataQualityIndicator'] = QInd
					if len(newDict.values()) != len(columns):
						print('not enough columns in final filtered string')
						with open(os.path.join(saveLocation, str(writtenFiles[count] + "_shortDict.csv")),
						          "a") as error2:
							error2.write(list(newDict.values()))
							error2.write('\n')
						continue
					else:
						writer.writerow(list(newDict.values()))
			print("Finished data retrieval for the %s table" %table)
	cnxn.close()
	return writtenFiles


###############################################################################
#########################        Dictionaries for QA codes 		###############
###############################################################################

#These are dictionaries that hold the code and the Tier level of the data...
QA_Code_list = {"AWM": 1,"AY": 2,"BB": 2,"BBM": 2,"BCQ": 1,"BE": 2,"BH": 1,"BLM": 3,"BRKA": 2,"BS": 2,"BT": 4,"BV": 3,"BX": 3,"BY": 3,"BZ": 3,"BZ15": 2,"C": 1,"CE": 3,"CIN": 2,"CJ": 2,"CNP": 2,"CQA": 1,"CS": 2,"CSG": 2,"CT": 2,"CVH": 1,"CVHB": 3,"CVL": 1,"CVLB": 3,"CZM": 2,"D": 1,"DB": 2,"DBLOD": 2,"DBM": 2,"DF": 2,"DG": 1,"DO": 1,"DRM": 2,"DS": 1,"DT": 1,"ERV": 3,"EUM": 3,"EX": 3,"F": 2,"FCL": 2,"FDC": 2,"FDI": 2,"FDO": 4,"FDP": 2,"FDR": 1,"FDS": 1,"FEU": 4,"FIA": 4,"FIB": 3,"FIF": 4,"FIO": 3,"FIP": 3,"FIT": 2,"FIV": 3,"FLV": 4,"FNM": 4,"FO": 2,"FS": 4,"FTD": 4,"FTT": 4,"FUD": 4,"FX": 3,"GB": 2,"GBC": 3,"GC": 1,"GCA": 1,"GD": 1,"GN": 3,"GR": 3,"H": 2,"H22": 3,"H24": 3,"H8": 2,"HB": 2,"HD": 3,"HH": 2,"HNO2": 2,"HR": 1,"HS": 3,"HT": 1,"IE": 2,"IF": 2,"IL": 3,"ILM": 2,"ILN": 2,"ILO": 2,"IM": 2,"IP": 3,"IP5": 3,"IPMDL2": 3,"IPMDL3": 3,"IPRL": 3,"IS": 3,"IU": 3,"IZM": 2,"J": 2,"JA": 2,"JDL": 2,"LB": 2,"LC": 3,"LRGN": 4,"LRIL": 4,"LRIP": 4,"LRIU": 4,"LRJ": 4,"LRJA": 4,"LRM": 4,"LRQ": 4,"LST": 4,"M": 2,"MAL": 1,"MN": 3,"N": 2,"NAS": 2,"NBC": 2,"NC": 1,"NG": 1,"NMDL": 1,"None": 1,"NR": 4,"NRL": 1,"NTR": 1,"OA": 2,"OV": 2,"P": 3,"PG": 3,"PI": 3,"PJ": 1,"PJM": 1,"PJN": 1,"PP": 3,"PRM": 3,"Q": 3,"QAX": 1,"QG": 3,"R": 4,"RE": 1,"REL": 1,"RIP": 4,"RIU": 4,"RJ": 4,"RLST": 4,"RPV": 3,"RQ": 2,"RU": 3,"RY": 3,"SC": 1,"SCR": 2,"SLM": 1,"TA": 3,"TAC": 1,"TC": 3,"TCI": 3,"TCT": 3,"TD": 3,"TH": 3,"THS": 3,"TK": 3,"TL": 2,"TNC": 2,"TNS": 1,"TOQ": 3,"TP": 3,"TR": 4,"TS": 3,"TW": 2,"UF": 2,"UJ": 2,"UKM": 3,"ULM": 3,"UOL": 2,"VCQ": 2,"VQN": 2,"VC": 2,"VBB": 2,"VBS": 2,"VBY": 3,"VBZ": 3,"VBZ15": 2,"VCJ": 2,"VCO": 2,"VCR": 2,"VD": 1,"VDO": 1,"VDS": 1,"VELB": 1,"VEUM": 3,"VFDP": 2,"VFIF": 4,"VFNM": 4,"VFO": 2,"VGB": 2,"VGBC": 3,"VGN": 3,"VH": 2,"VH24": 3,"VH8": 2,"VHB": 2,"VIE": 2,"VIL": 3,"VILN": 3,"VILO": 2,"VIP": 3,"VIP5": 3,"VIPMDL2": 3,"VIPMDL3": 3,"VIPRL": 3,"VIS": 3,"VIU": 3,"VJ": 2,"VJA": 2,"VLB": 2,"VLMQO": 2,"VM": 2,"VNBC": 2,"VNC": 1,"VNMDL": 1,"VNTR": 1,"VPJM": 1,"VPMQO": 2,"VQAX": 1,"VQCA": 3,"VQCP": 3,"VR": 4,"VRBS": 4,"VRBZ": 4,"VRDO": 4,"VRE": 1,"VREL": 1,"VRGN": 4,"VRIL": 4,"VRIP": 4,"VRIU": 4,"VRJ": 4,"VRLB": 4,"VRLST": 4,"VRQ": 2,"VRVQ": 4,"VS": 2,"VSC": 1,"VSCR": 2,"VSD3": 1,"VTAC": 1,"VTCI": 3,"VTCT": 3,"VTNC": 2,"VTOQ": 3,"VTR": 4,"VTW": 3,"VVQ": 4,"WOQ": 3}

BatchVerificationCode_list = {"NA": 3,"NR": 3,"VAC": 1,"VAC:VCN": 4,"VAC:VMD": 2,"VAC:VMD:VQI": 3,"VAC:VQI": 3,"VAC:VR": 4,"VAF": 1,"VAF:VMD": 2,"VAF:VQI": 3,"VAP": 1,"VAP:VI": 3,"VAP:VQI": 3,"VCN": 4,"VLC": 1,"VLC:VMD": 2,"VLC:VMD:VQI": 3,"VLC:VQI": 3,"VLF": 1,"VMD": 2,"VQI": 3,"VQI:VTC": 3,
"VQN": 3,"VR": 4,"VTC": 2}

ResultQualCode_list = {"/oC": 4,"<": 1,"<=": 1,"=": 1,">": 1,">=": 1,"A": 1,"CG": 4,"COL": 1,"DNQ": 1,"JF": 1,"NA": 4,"ND": 1,"NR": 4,"NRS": 4, "NRT": 4, "NSI": 1, "P": 1, "PA": 1, "w/C": 4, "": 1,}


TargetLatitude_list = {"-88": 4, "": 4, }
Result_list = {"":4}


StationCode_list = {"LABQA": 0,"LABQA_SWAMP": 0,"000NONPJ": 0,"FIELDQA": 0,"Non Project QA Sample": 0,"Laboratory QA "
                    "Sample": 0, "Field QA sample": 0}
SampleTypeCode_list = {"LabBlank": 0, "CompBLDup": 0, "LCS": 0, "CRM": 0, "FieldBLDup_Grab": 0, "FieldBLDup_Int": 0, "FieldBLDup": 0, "FieldBlank": 0, "TravelBlank": 0, "EquipBlank": 0, "DLBlank": 0, "FilterBlank": 0, "MS1": 0, "MS2": 0, "MS3": 0, "MSBLDup": 0}
ProgramName_list = {}
Analyte_list = {"Surrogate":0, }
MatrixName_list = {"blankwater": 0,  "Blankwater": 0,  "labwater": 0,  "blankmatrix": 0, }
CollectionReplicate_list = {"0": 1, "1": 1, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0,  "7": 0,  "8": 0, }
ResultsReplicate_list = {"0": 1, "1": 1, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0,  "7": 0,  "8": 0, }

Codes_Dict = {"QACode": QA_Code_list,
			"BatchVerification": BatchVerificationCode_list,
			"ResultQualCode": ResultQualCode_list,
			"TargetLatitude": TargetLatitude_list,
			"Result": Result_list,
			"StationCode": StationCode_list,
			"SampleTypeCode": SampleTypeCode_list,
			"ProgramName": ProgramName_list,
			"Analyte": Analyte_list,
			"MatrixName": MatrixName_list,
			"CollectionReplicate": CollectionReplicate_list,
			"ResultsReplicate": ResultsReplicate_list, }

#tables = {"WaterChemistryData": "WQDMart_MV", "ToxicityData": "ToxDmart_MV", "TissueData": "TissueDMart_MV",
# "BenthicData": "BenthicDMart_MV", "HabitatData": "HabitatDMart_MV"}
tables = { "IR_ToxicityData": "IR2018_Toxicity", "IR_BenthicData": "IR2018_Benthic", "IR_WaterChemistryData":
	"IR2018_WQ", }

###########################################################################################################################
#########################        Dictionaries for QA codes 		###########################
###########################################################################################################################



##############################################################################
########################## Main Statement  ###################################
##############################################################################

# Necessary variables imported from user's environmental variables.
user = os.environ.get('DCG_user')
password = os.environ.get('DCG_pw')
URI = os.environ.get('URI')
SERVER1 = os.environ.get('SERVER1')
UID = os.environ.get('UID')
PWD = os.environ.get('PWD')

IR_PWD = os.environ.get('IR_PWD')
SERVER_IR = os.environ.get('IR_SERVER')
IR_UID = os.environ.get('IR_UID')

StartYear = 1994
EndYear = 2017
saveLocation = "S:\\DWQ\\DIV\\tmdls\\MALU\\2018 Integrated Report\\Temporary"
FILES = data_retrieval(tables, StartYear, EndYear, saveLocation)


###################################################################################################
#################################        Push data to data.ca.gov 		############################################
###################################################################################################
# NODE Testing
# 1912 Data Update Automation, Large File Loading

# 1906 CEDEN Chemistry Data
#  541 Surface Water Toxicity
#  431 CEDEN Benthic Data
#  451 CEDEN Tissue Data
# 2011 CEDEN Habitat Data
# 1911 CEDEN Safe to Swim



#NODE = 1912


fileWritten = FILES[1]


# fileWritten = 'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\ToxicityData_2000-2017.txt'
# Attach dataset data
try:
	#Sign into the data.ca.gov website
	print("Connecting to data.ca.gov")
	#api = DatasetAPI(URI, user, password )
	print("Connected")
	print("Pushing data")
	#r = api.attach_file_to_node(file=fileWritten, node_id=NODE, field='field_upload' )
except:
	print('need this line')

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





import pyodbc, argparse
#import pandas as pd
#import numpy as np
import os, csv, re
import time
#import json
from dkan.client import DatasetAPI



IR_PWD = os.environ.get('IR_PWD')
SERVER_IR = os.environ.get('IR_SERVER')
IR_UID = os.environ.get('IR_UID')

cnxn = pyodbc.connect(Driver='SQL Server Native Client 11.0', Server=SERVER_IR, uid=IR_UID, pwd=IR_PWD)
cursor = cnxn.cursor()
sql = "SELECT * FROM %s ;" % "IR2018_WQ"

cursor.execute(sql)
columns = [desc[0] for desc in cursor.description]
for column in columns:
	print(column)