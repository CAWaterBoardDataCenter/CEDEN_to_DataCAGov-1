import pyodbc, argparse
import pandas as pd
import numpy as np


#StartYear=2016
#Endyear=2017
table="WQDMart_MV"

# Necessary variables imported from user's environmental variables. 
user = os.environ.get('DCG_user')
password = os.environ.get('DCG_pw')
URI= os.environ.get('URI')
SERVER1 = os.environ.get('SERVER1')
UID = os.environ.get('UID')
PWD = os.environ.get('PWD')

cnxn = pyodbc.connect(Driver='SQL Server Native Client 11.0', Server=SERVER1, uid=UID, pwd=PWD)
cursor = cnxn.cursor()

# could use this method to test whether a table exists as a user input....
cursor.execute("select * from sys.tables")
rows = cursor.fetchall()
for row in rows:
	print(row[0])



#	sql = ("select type_desc, physical_name from %s.sys.database_files" % row.name)
#	cursor.execute(sql)
#	line1rows = cursor.fetchall()
#	print("Files for " + row.name + " :")
#	for l1row in line1rows:
#		print l1row.type_desc + " " + l1row.physical_name
