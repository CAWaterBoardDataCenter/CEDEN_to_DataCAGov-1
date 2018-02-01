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
import os
import csv
import re
from datetime import datetime
import string
from dkan.client import DatasetAPI

printable = set(string.printable) - set('|"\'`\t\r\n\f\v')
SERVER = os.environ.get('FHAB_Server')
UID = os.environ.get('FHAB_User')
path = 'C:\\Users\AHill\Documents\FHABs'
FHAB = 'FHAB'
ext = '.txt'
sep = '\t'
file = os.path.join(path, FHAB + ext)


# decodeAndStrip takes a string and filters each character through the printable variable. It returns a filtered string.
def decodeAndStrip(t):
	filter1 = ''.join(filter(lambda x: x in printable, str(t)))
	return filter1

cnxn = pyodbc.connect(Driver='SQL Server Native Client 11.0', Server=SERVER, uid=UID, Trusted_Connection='Yes')
cursor = cnxn.cursor()
sql = "SELECT dbo.AlgaeBloomReport.AlgaeBloomReportID, dbo.AlgaeBloomReport.RegionalBoardID, dbo.AlgaeBloomReport.CountyID," \
      " dbo.AlgaeBloomReport.Latitude, dbo.AlgaeBloomReport.Longitude, dbo.AlgaeBloomReport.ObservationDate, CASE WHEN " \
      "HasPostedSigns = 1 THEN 'Yes' ELSE 'No' END AS HasPostedSigns, CASE WHEN HasContactWithWater = 1 THEN 'Yes' ELSE 'No' " \
      "END AS HasContactWithWater, dbo.AlgaeBloomReport.WaterBodyType, dbo.AlgaeBloomReport.WaterBodyName, " \
      "dbo.AlgaeBloomReport.WaterBodyManager, dbo.AlgaeBloomReport.RecLandManager, CASE WHEN IsIncidentResoloved = 1 THEN 'Yes'" \
      " ELSE 'No' END AS IsIncidentResoloved, dbo.AlgaeBloomReport.IncidentInformation, dbo.AlgaeBloomReport.TypeofSign, " \
      "dbo.AlgaeBloomReport.OfficialWaterBodyName, dbo.AlgaeBloomReport.BloomLastVerifiedOn, dbo.AlgaeBloomReport.BloomDeterminedBy, " \
      "dbo.AlgaeBloomReport.ApprovedforPost FROM (dbo.AlgaeBloomReport INNER JOIN dbo.County ON dbo.AlgaeBloomReport.CountyID = dbo.County.CountyID) " \
      "INNER JOIN dbo.RegionalBoard ON dbo.AlgaeBloomReport.RegionalBoardID = dbo.RegionalBoard.RegionalBoardID " \
      "WHERE (((dbo.AlgaeBloomReport.ApprovedforPost)= 1))"
cursor.execute(sql)
columns = [desc[0] for desc in cursor.description]
with open(file, 'w', newline='', encoding='utf8') as writer:
	dw = csv.DictWriter(writer, fieldnames=columns, delimiter=sep, lineterminator='\n')
	dw.writeheader()
	FHAB_writer = csv.writer(writer, csv.QUOTE_MINIMAL, delimiter=sep, lineterminator='\n')
	for row in cursor:
		row = [str(word).replace('None', '') for word in row]
		filtered = [decodeAndStrip(t) for t in list(row)]
		newDict = dict(zip(columns, filtered))
		FHAB_writer.writerow(list(newDict.values()))

# 2156 FHAB portal data

NODE = 2156

user = os.environ.get('DCG_user')
password = os.environ.get('DCG_pw')
URI = os.environ.get('URI')

api = DatasetAPI(URI, user, password, debug=False)

r = api.attach_file_to_node(file=file, node_id=NODE, field='field_upload', update=0)



