import os, sys, arcpy, uuid
import traceback
import logging
import pyodbc

import util

###################################################################################################
###################################################################################################
#
# class:	db.Replicas
# purpose:	Class that parses a Python dictionary and extract an array of replicas.
#
# author:	Jason Sardano
# date:		Sep 28, 2013
#
# notes:	Need to install 32-bit Python ODBC client (pyodbc), 64-bit doesn't work with ESRI's python installation
#
###################################################################################################

class Replicas(object):
	#replicaConfigs: An array of Replica configs, see Replica below.
	def __init__(self, replicaConfigs):
		self.replicas = []
		for i in range(0, len(replicaConfigs)):
			replicaConfig = replicaConfigs[i]
			replica = Replica(replicaConfig)
			if replica.disabled:
				logging.info(replica.name + ' is disabled')
			else:
				logging.info('Adding ' + replica.name)
				self.replicas.append(replica)
		return
		
###################################################################################################
###################################################################################################
#
# class:	db.Replica
# purpose:	Class that parses a Python dictionary, extracts information about the replica,
#			and extracts an array of datasets.
#
# author:	Jason Sardano
# date:		Sep 28, 2013
#
###################################################################################################

class Replica(object):
	
	#config: A Python dictionary that has the following structure:
	#{
	#	"name":"DBO.StagingToProduction",
	#	"disabled":False,
	#	"sqlServer": {
	#		"server":"arcgis10.arbweb.harvard.edu",
	#		"database":"Warehouse"
	#	},
	#	"tempPath":r"C:\Users\Public\Documents\BGBase Connector\temp",
	#	"exportPath":r"C:\Users\Public\Documents\BGBase Connector\temp",
	#	"lockFilePath":r"C:\Users\Public\Documents\BGBase Connector\temp\bgimport.loc",
	#	"deleteTempFiles":True,
	#	"autoReconcile":True,
	#	"stagingWorkspace":"C:\Users\Public\Documents\SDE Connections\Staging@ARCGIS10.sde",
	#	"productionWorkspace":"C:\Users\Public\Documents\SDE Connections\Production@ARCGIS10.sde",
	#	"sqlserverEditVersion":"DBO.BG-BASE",
	#	"stagingEditVersions":["DBO.DESKTOP","DBO.MOBILE"],
	#	"stagingDefaultVersion":"dbo.DEFAULT",
	#	"datasets":[array of Dataset config, see the Dataset class]
	#}
	def __init__(self, config):
		self.name = config['name']
		self.datasets = []
		
		if 'disabled' in config:
			self.disabled = config['disabled']
		else:
			self.disabled = False
		
		self.tempPath = config['tempPath']
		self.exportPath = config['exportPath']
		self.lockFilePath = config['lockFilePath']
		self.deleteTempFiles = config['deleteTempFiles']
		self.autoReconcile = config['autoReconcile']
		self.stagingWorkspace = config['stagingWorkspace']
		self.productionWorkspace = config['productionWorkspace']
		self.sqlserverEditVersion = config['sqlserverEditVersion']
		self.stagingEditVersions = config['stagingEditVersions']
		self.stagingDefaultVersion = config['stagingDefaultVersion']
		
		server = config['sqlServer']['server']
		database = config['sqlServer']['database']
		self._connectionString = "DRIVER={SQL Server};SERVER=${server};DATABASE=${database};Trusted_Connection=yes".replace('${server}', server).replace('${database}', database)
		
		self._connection = None
		self.dbutil = util.DBUtil()

		if not self.disabled:
			for i in range(0, len(config['datasets'])):
				dataset = Dataset(config['datasets'][i], self)
				if dataset.disabled:
					logging.info(str(dataset) + ' is disbled.')
				else:
					self.datasets.append(dataset)
		return
		
	def __del__(self):
		if self._connection:
			self.close(self._connection)
		return
		
	def __str__(self):
		return self.name
		
	def close(self, dbobject):
		if self.dbutil and dbobject is not None:
			self.dbutil.close(dbobject)
		return
		
	def isConnected(self):
		return not self._connection is None
		
	def connect(self):
		func = 'Replica._connect'
		try:
			self._connection = pyodbc.connect(self._connectionString)
			return True
		except:
			self._connection = None
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
		return False
		
	def closeConnection(self):
		if self.isConnected():
			try:
				self._connection.close()
				self._connection = None
			except:
				tb = sys.exc_info()[2]
				tbinfo = traceback.format_tb(tb)[0]
				msg = "Error in close:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
				logging.error(msg);
		return
		
	def getConnection(self):
		return self._connection

###################################################################################################
###################################################################################################
#
# class:	db.Dataset
# purpose:	Class that parses a Python dictionary and extracts information about the dataset.
#
# author:	Jason Sardano
# date:		Sep 28, 2013
#
###################################################################################################

class Dataset(object):
	#config: A Python dictionary that has the following structure:
	#	{
	#		"cdcFunction":"cdc.fn_cdc_get_all_changes_dbo_PLANTS_LOCATION",
	#		"sqlserverDataset":
	#		{
	#			"table":"Warehouse.cdc.dbo_PLANTS_LOCATION_CT",
	#			"primaryKey":"rep_id",
	#			"xField":"X_COORD",
	#			"yField":"Y_COORD"
	#		},
	#		"sdeDataset":
	#		{
	#			"table":"Staging.dbo.PLANTS_LOCATION",
	#			"primaryKey":"rep_id"
	#		}
	#	}
	#
	#replica: The parent Replica object
	def __init__(self, config, replica):
		self.replica = replica
		if 'disabled' in config:
			self.disabled = config['disabled']
		else:
			self.disabled = False
		self.cdcFunction = config['cdcFunction']
		self.cdcTable = config['sqlserverDataset']['table']
		self.cdcPrimaryKey = config['sqlserverDataset']['primaryKey']
		self.isSpatial = ('xField' in config['sqlserverDataset']) and ('yField' in config['sqlserverDataset'])
		if self.isSpatial:
			self.xField = config['sqlserverDataset']['xField']
			self.yField = config['sqlserverDataset']['yField']
		self.sdeTable = config['sdeDataset']['table']
		self.sdePrimaryKey = config['sdeDataset']['primaryKey']
		
		self._changeCursor = None
		self._changeCursorFields = None
		
		return
		
	def __str__(self):
		return self.cdcTable + '->' + self.sdeTable;
		
	########################################################################
	# Executes the CDC function and returns a cursor of CDC records for the dataset.
	def getChanges(self):
		func = 'Dataset.getChanges'
		try:
			self.replica.close(self._changeCursor)
			self._changeCursorFields = None
			
			if not self.replica.isConnected():
				logging.error(func + ': No connection')
				return None
		
			dateUtil = util.DateUtil()
			now = dateUtil.now()
			self._changeCursor = self.replica.getConnection().cursor()
		
			sql = '''
			DECLARE @begin_time datetime, @end_time datetime, @begin_lsn binary(10), @end_lsn binary(10);
SET @begin_time = \'2001-01-01 00:00:01\';
SET @end_time = \'''' + now + '''\';
SELECT @begin_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than', @begin_time);
SELECT @end_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);
SELECT *, CONVERT(VARCHAR(MAX), __$seqval, 2) as __$CDCKEY FROM ''' + self.cdcFunction + '''(@begin_lsn, @end_lsn, 'all');
'''
			self._changeCursor.execute(sql)
			self._changeCursorFields = self.replica.dbutil.getColumns(self._changeCursor)
			
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
			return None
		
		return self._changeCursor
		
	def getChangeFields(self):
		return self._changeCursorFields
		
	########################################################################
	# Determine the database operation type of the row.
	# returns "insert","update","delete"
	def getOperationType(self, cdcRow):
		func = "Dataset.getOperationType"
		op = "";
		try:
			operation = cdcRow[self._changeCursorFields["__$operation"]]
			if operation == 1:
				op = "delete"
			elif operation == 2:
				op = "insert"
			elif operation == 4:
				op = "update"
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
		return op
	
	########################################################################
	# Deletes records from the CDC table of records that were processed.
	# processedRecords: An array of CDC IDs
	def clearChanges(self, processedRecords):
		logging.info('Clearing changes from CDC tables for ' + self.cdcTable)
		func = "Database.clearChanges"
		try:
			cursor = self.replica.getConnection().cursor()
			ids = ""
			for i in range(0, len(processedRecords)):
				if i > 0:
					ids = ids + ","
				ids = ids + "'" + processedRecords[i] + "'"
			sql = 'DELETE FROM ' + self.cdcTable + ' where CONVERT(VARCHAR(MAX), __$seqval, 2) in (' + ids + ')'
			cursor.execute(sql)
			self.replica.getConnection().commit()
			logging.debug('Deleted ' + str(cursor.rowcount) + ' rows from ' + self.cdcTable)
			cursor.close()
			del cursor
			cursor = None
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
		return
		
	def getSdeTablePath(self):
		return os.path.join(self.replica.stagingWorkspace, self.sdeTable)

	def makeLayer(self, key):
		where_clause = self.sdePrimaryKey + " = " + str(key)
		feature_class = self.getSdeTablePath()
		layer_name = "lyr" + str(uuid.uuid1()).replace("-", "")
		arcpy.MakeFeatureLayer_management(feature_class, layer_name, where_clause)
		arcpy.ChangeVersion_management(layer_name,'TRANSACTIONAL', self.replica.sqlserverEditVersion,'')
		return layer_name