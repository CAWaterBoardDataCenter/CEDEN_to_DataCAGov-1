'''

Purpose:
This script is intended to be written for a specific dataset such that the user only needs to input a final file name and the date range in years (ie "2000 2006"). The script will use the predefined SQL statement (that was custom coded into this script) to query an internal waterboard data mart of CEDEN data. The script will write this query result to a csv file and then interact with the data.ca.gov api to upload this data to an existing "resource". The name of the csv file will be seen by the public and its description should be chosen carefully. This script can be copied to a new filename and the SQL statement altered to query a different dataset on the Waterboards data mart. Please note that there are 5 data marts available but the defualt is the "WQDMart_MV" (other data marts include data on toxicity, Tissue, Benthic, and Habitat data. These can be specified with the "-T" flag. 

In an interest in making data open, machine readable, and transparent, please perform any and all additional data adjustments in the section called "Data fixes and alterations".


'''

import pyodbc, argparse
import pandas as pd
import numpy as np
import os, csv
import json
from dkan.client import DatasetAPI

# Necessary variables imported from user's environmental variables. 
user = os.environ.get('DCG_user')
password = os.environ.get('DCG_pw')
URI= os.environ.get('URI')
SERVER1 = os.environ.get('SERVER1')
UID = os.environ.get('UID')
PWD = os.environ.get('PWD')


# other table option can include
# WQDMart_MV, ToxDmart_MV, TissueDMart_MV, BenthicDMart_MV, HabitatDMart_MV 

# Example could be with all options specified
# python .\WorkingScripts\CEDEN_to_DataCAgov_SafeToSwim.py -f CEDEN_SafeToSwimData -t 2000 2017 -n 1841


# import argument parser. This will allow a user to specify mandatory options on the command line by using a flag ("-Flag") followed by a non-quoted text. See example above. 
# Though the "-S" flag can be specified, it does not need to be and the default value will work. 
# Change the table value on the command line for toxicity, Tissue, Benthic, or Habitat data. 
# Use the "-t" flag to specify a time range in years (inclusive) of data to be returned. EX, "2016 2016" will return data between 01/01/2016 to 12/31/2016.
# Use the "-f" flag to specify the beginning of the filename. Your input will be appended with the table name and date range. EX, "Output" will become "Output_WQDmart_2016-2016.csv"
parser = argparse.ArgumentParser(description='SQL Server object search. Case sensitive')
parser.add_argument('-S', '--server', help='Instance you wish to connect to.', dest="instance", default=SERVER1)
parser.add_argument('-T', '--table', help='Table options for this script include: WQDMart_MV, ToxDmart_MV, TissueDMart_MV, BenthicDMart_MV, HabitatDMart_MV', dest="table", default='WQDMart_MV')
parser.add_argument('-t', '--TimeSpan', help='Range of years separated by a single space', dest="TimeSpan", nargs=2, default=("2016","2016"))
parser.add_argument('-f', '--fileName', help='Name of output file. Time span as suffix will be automatically included ( if you enter "MyFile" the file name will become "MyFile_table_2016-2016.csv.gz"', dest="fileName",default="Output.csv")
parser.add_argument('-n', '--node', help='The data.ca.gov Node you wish to update... Be very careful this will overwrite the existing data with no prompt!!!!!!!', dest="node", required=True)
argList = parser.parse_args()


Server1=argList.instance
table=argList.table
fileName = argList.fileName
StartYear= int(argList.TimeSpan[0])
EndYear = int(argList.TimeSpan[1])
NODE = int(argList.node)

#############################################
'''
########################			!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!		#####################
table= 'WQDMart_MV'; fileName = 'CEDEN_SafeToSwimData'; StartYear = 2000; EndYear = 2017; NODE = 1841


########################			!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!		#####################
'''

try:
	cnxn = pyodbc.connect(Driver='SQL Server Native Client 11.0', Server=SERVER1, uid=UID, pwd=PWD)
except:
	print("Couldn't connect to %s. It is down or you might have had a typo. Check internet connection." % argList.instance)

# a python cursor is a synonym to a recordset or resultset.
cursor = cnxn.cursor()


#######################################################################################
#########################        SQL section to be modified for every new dataset		#############################
#######################################################################################

sql=("SELECT %s.Program, %s.ParentProject, %s.Project, %s.StationName, %s.TargetLatitude, %s.TargetLongitude, %s.StationCode, %s.SampleDate, %s.SampleTypeCode, %s.LabSampleID, %s.MatrixName, %s.Analyte, %s.Unit, %s.Result, " \
"%s.MDL, %s.RL, %s.ResultQualCode, %s.QACode, %s.BatchVerification " \
"FROM %s WHERE NOT " \
"(StationCode = 'LABQA' OR StationCode = 'LABQA_SWAMP' " \
"OR Result = '' OR TargetLatitude=-88 OR TargetLatitude='' " \
"OR SampleTypeCode = 'LabBlank' " \
"OR SampleTypeCode = 'LCS' " \
"OR SampleTypeCode = 'CRM' " \
"OR SampleTypeCode = 'FieldBLDup_Grab' " \
"OR SampleTypeCode = 'FieldBLDup_Int' " \
"OR SampleTypeCode = 'FieldBLDup' " \
"OR SampleTypeCode = 'FieldBlank' " \
"OR SampleTypeCode = 'MS3' " \
"OR SampleTypeCode = 'TravelBlank' " \
"OR SampleTypeCode = 'EquipBlank' " \
"OR SampleTypeCode = 'DIBlank' " \
"OR SampleTypeCode = 'FilterBlank' " \
"OR SampleTypeCode = 'MS1' " \
"OR SampleTypeCode = 'MS2' " \
"OR SampleTypeCode = 'MSBLDup' " \
"OR MatrixName ='blankwater' " \
"OR MatrixName ='Blankwater' " \
"OR MatrixName ='labwater' " \
"OR MatrixName ='blankmatrix') " \
"AND " \
"(Analyte = 'E. Coli' OR Analyte='Enterococcus' " \
"OR Analyte = 'Coliform, Total' OR Analyte = 'Coliform, Fecal') " \
"AND (CollectionReplicate = 1 AND ResultsReplicate = 1) " \
"AND (SampleDate BETWEEN CONVERT(datetime, '%d-01-01') AND CONVERT(datetime, '%d-12-31'));") % (table,table,table,table,table,table,table,table,table,table,table,table,table,table,table,table,table,table,table, table, StartYear, EndYear)

#######################################################################################
#########################        SQL section to be modified for every new dataset		#############################
#######################################################################################


##################################################
######		 Import SQL statement into the cursor object. 		##########
##################################################
cursor.execute(sql)
print("Starting data retrieval")
#data = cursor.fetchall()

fileWritten ='%s, %d-%d.csv' %(fileName, StartYear, EndYear)
columns = [desc[0] for desc in cursor.description]

with open(fileWritten, 'w', newline='') as csvfile:
	dw = csv.DictWriter(csvfile, fieldnames=columns)
	dw.writeheader()
	csv.writer(csvfile, csv.QUOTE_MINIMAL).writerows(cursor)


print("Finished data retrieval")


###################################################################################################
#################################        Push data to data.ca.gov 		############################################
###################################################################################################
print("Opening connection to data.ca.gov")
# Attach dataset data 
try:
	#Sign into the data.ca.gov website
	api = DatasetAPI(URI, user, password )
	
	# Attach file to node on data.ca.gov
	
	###################################################################################################################
	###################################################################################################################
	#####################		Be Very Careful here 	Make Sure you have the right Node value		###############################################
	###################################################################################################################
	###################################################################################################################
	# node # 1841 corresponds to "Sample Data... Not to be used for decision making" resource on the "Data Update Automation" dataset

	print("Connection open and pushing data")
	r = api.attach_file_to_node(file = fileWritten, node_id=NODE, field = 'field_upload' )
	print("Upload completed successfully")
	# Good for inspecting features of dataset/resource.
	#r = api.node('retrieve', node_id=NODE)
	#print(r.json())
	# use following line to inspect full response
	# r.json()
	# r.json()['nid'] #returns value for nid only. Other fields can be returned in this manner.


except:
	Print("Try... try again")

#os.remove(fileWritten)

cnxn.close()
del cnxn


