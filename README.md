Author:
	Andrew Dix Hill; https://github.com/AndrewDixHill/CEDEN_to_DataCAGov ; andrew.hill@waterboards.ca.gov

Agency:
	California State Water Resource Control Board (SWRCB)
	Office of Information Management and Analysis (OIMA)

Purpose:
	This script is intended to query, clean and calculate new fields for
datasets from an internal SWRCB DataMart of CEDEN data. This script also applies a data 
quality estimate to every record and cleans the original dataset of non-ASCII characters.
This data quality estimate is calculated from a data quality decision tree found here XXXX.

How to use this script:
	From a powershell prompt (windows), call python and specify
	the complete path to this file. Below is an example, where XXXXXXX should be replaced
	with the filename and the path should be specific to the file location:
	python C:\\Users\\AHill\\Downloads\\XXXXXXX.py

Prerequisites:
	Windows platform (not strictly a requirement but I was unable to get this library
		working on a mac... I tried)
	Python 3.X
	pyodbc library for python.  See https://github.com/mkleehammer/pyodbc
	dkan library for python.    See https://github.com/GetDKAN/pydkan

Scripts in the "WorkingScripts" folder can be repurposed but are not supported or intended for 
use outside of testing.
