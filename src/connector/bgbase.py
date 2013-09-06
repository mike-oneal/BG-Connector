import os, sys, arcpy
import traceback
import logging
import pyodbc

import util

###################################################################################################
###################################################################################################
#
# class:	Warehouse.py
# purpose:	Class that encapsulates record from the BG-BASE Warehouse.
#
# author:	Jason Sardano
# date:		Aug 10, 2013
#
# notes:	This script encapsulates a table in SQL Server that maintains a list of
#			tables/features classes that are to be synchronized between BG-BASE and
#			ArcGIS products. The schema of the table is as follows:
#
# notes:	Need to install 32-bit Python ODBC client (pyodbc), 64-bit doesn't work with ESRI's python installation
#
#	TABLE_NAME: 	Name of the table in the ArcGIS database.
#	CDC_TABLE_NAME:	Name of the table in the SQL Server warehouse database.
#	CDC_FUNCTION:	The name of the function that the scripts will call to get the changed records.
#	PK_FIELD:		The primary key field name in the Warehouse table and ArcGIS table.
#	X_FIELD:		Optional. The field name that contains the Longitude data for the table.
#	Y_FIELD:		Optional. The field name that contains the Latitude data for the table.
#	DISABLED:		If set to 1, then the script will ignore this table.
#	LAST_SYNC_DATE:	The last time this table was synced.
#
###################################################################################################

class Warehouse(object):
	#server:		name of the SQL Server.
	#database:		name of the database that contains our table.
	#adminTable:	name of the admin table.
	def __init__(self, server, database, adminTable):
		self._server = server
		self._database = database
		self._connectionString = "DRIVER={SQL Server};SERVER=${server};DATABASE=${database};Trusted_Connection=yes".replace('${server}', server).replace('${database}', database)
		self._adminTable = adminTable
		self._connection = None
		self._changeCursor = None
		self._changeCursorFields = None
		self._dbutil = util.DBUtil()
		self._connect()
		
	def __del__(self):
		self._dbutil.close(self._changeCursor)
		self._dbutil.close(self._connection)
		
	def isConnected(self):
		return not self._connection is None
		
	def _connect(self):
		func = 'Warehouse._connect'
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
	
	def close(self):
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
	
	def getSyncDatasets(self):
		datasets = []
		func = 'Warehouse.getSyncDatasets';
		cursor = None
		try:
			if(not self.isConnected()):
				logging.error(func + ': No connection')
			else:
				sql = 'SELECT * FROM ${table}'.replace('${table}', self._adminTable)
				cursor = self._connection.execute(sql)
				fields = self._dbutil.getColumns(cursor)
				for row in cursor:
					dataset = dict()
					dataset['table'] = row[fields['TABLE_NAME']]
					dataset['func'] = row[fields['CDC_FUNCTION']]
					dataset['last_run'] = row[fields['LAST_SYNC_DATE']]
					dataset['cdc_table'] = row[fields['CDC_TABLE_NAME']]
					dataset['disabled'] = row[fields['DISABLED']] == 1
					dataset['pkfield'] = row[fields['PK_FIELD']]
					dataset['xfield'] = row[fields['X_FIELD']]
					dataset['yfield'] = row[fields['Y_FIELD']]
			
					str_last_run = '2000-01-01 00:00:00'
					last_run = dataset['last_run']
					if last_run is None:
						dataset['last_run'] = str_last_run
					else:
						str_last_run = last_run.strftime('%Y-%m-%d %H:%M:%S')
					dataset['last_run'] = str_last_run
					datasets.append(dataset)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
		finally:
			self._dbutil.close(cursor)
		
		return datasets
	
	#Given an input dataset, returns a cursor of CDC records for that dataset
	def getChanges(self, dataset):
		func = 'Warehouse.getChanges'
		try:
			self._dbutil.close(self._changeCursor)
			self._changeCursorFields = None
			
			if not self.isConnected():
				logging.error(func + ': No connection')
				return None
		
			dateUtil = util.DateUtil()
			now = dateUtil.now()
			dataset['read_time'] = now
			self._changeCursor = self._connection.cursor()
		
			sql = '''DECLARE @begin_time datetime, @end_time datetime, @begin_lsn binary(10), @end_lsn binary(10);
SET @begin_time = \'''' + dataset['last_run'] + '''\';
SET @end_time = \'''' + now + '''\';
SELECT @begin_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than', @begin_time);
SELECT @end_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);
SELECT *, CONVERT(VARCHAR(MAX), __$seqval, 2) as __$CDCKEY FROM ''' + dataset['func'] + '''(@begin_lsn, @end_lsn, 'all');''' 
			self._changeCursor.execute(sql)
			self._changeCursorFields = self._dbutil.getColumns(self._changeCursor)
			
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
			return None
		
		return self._changeCursor
	
	#returns "insert","update","delete"
	def getOperationType(self, cdcRow):
		func = "Warehouse.getOperationType"
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
	
	def clearChanges(self, deleteList, datasets):
		logging.info('Clearing changes from CDC tables')
		func = "Warehouse.clearChanges"
		try:
			cursor = self._connection.cursor()
			for table in deleteList:
				ids = ''
				for i in range(0, len(deleteList[table])):
					if i > 0:
						ids = ids + ","
					ids = ids + "'" + deleteList[table][i] + "'"
				logging.debug('Clearing changes from ' + table)
				sql = 'DELETE FROM ' + table + ' where CONVERT(VARCHAR(MAX), __$seqval, 2) in (' + ids + ')'
				cursor.execute(sql)
				self._connection.commit()
				logging.debug('Deleted ' + str(cursor.rowcount) + ' rows from ' + table)
			
			if datasets is not None:
				for dataset in datasets:
					sql = "UPDATE ${table} SET LAST_SYNC_DATE = '${date}' WHERE CDC_TABLE_NAME = '${cdc_table}'".replace('${table}', self._adminTable).replace('${date}', dataset['read_time']).replace('${cdc_table}', dataset['cdc_table'])
					cursor.execute(sql)
					self._connection.commit()
			cursor.close()
			del cursor
			cursor = None
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg);
		return
