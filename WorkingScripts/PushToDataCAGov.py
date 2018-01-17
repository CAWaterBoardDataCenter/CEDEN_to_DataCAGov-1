
import os, csv
from dkan.client import DatasetAPI


###################################################################################################
#################################        Push data to data.ca.gov 		############################################
###################################################################################################
# NODE Testing
# 1912 Data Update Automation, Large File Loading
# 2061 Testing
# 2076 Keep Alive

# 1906 CEDEN Chemistry Data
#  541 Surface Water Toxicity
#  431 CEDEN Benthic Data
#  451 CEDEN Tissue Data
# 2011 CEDEN Habitat Data
# 1911 CEDEN Safe to Swim



NODE = 2076


#fileWritten = FILES[1]

user = os.environ.get('DCG_user')
password = os.environ.get('DCG_pw')
URI = os.environ.get('URI')
fileWritten = 'C:\\Users\\AHill\\Documents\\CEDEN_DataMart\\testExtraSmall.csv'
# Attach dataset data
#try:
	#Sign into the data.ca.gov website
	#print("Connecting to data.ca.gov")
api = DatasetAPI(URI, user, password, debug=False)
	#print("Connected")
	#print("Pushing data")
r = api.attach_file_to_node(file=fileWritten, node_id=NODE, field='field_upload', update=0)
''' Uploads and attaches a file to a specific node
 :param file: A path to a file
 :param node_id: The node id that we'll attach to the file to
 :param field: The name of the drupal file field
 :param update: (optional) 0 -> replace existing, 1 -> attach a new one
'''
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


