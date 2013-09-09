import os, sys, arcpy
import traceback, logging, uuid
import arcpy
import util

###################################################################################################
###################################################################################################
#
# class:	WarehouseToSde
# purpose:	Worker class that reads data from the Warehouse CDC records and updates SDE.
#			Refreshes the data in Staging SDE BG-BASE Version from the Warehouse Database.
#			Reconciles the data in the Staging BG-BASE Version to Staging SDE Default Version
#			Synchronizes the data from Staging SDE Default Version to Production SDE Default Version.
#
# author:	Jason Sardano
# date:		Aug 10, 2013
#
###################################################################################################


class WarehouseToSde(object):
	#warehouse:	Warehouse object that controls which records will be refreshed.
	#config: connector.util.Config object with the following keys:
	#	lockFilePath:			Path for lock file
	#	stagingWorkspace:		Path to the Staging Workspace
	#	productionWorkspace:	Path to the Production Workspace
	#	bgbaseEditVersion:		Version name to perform the edits in
	#	replica:				Name of the the replica to sync.

	def __init__(self, warehouse, config):
		self._warehouse = warehouse
		self._config = config
		self._dbutil = util.DBUtil()
		
	def run(self):
		func = 'WarehouseToSde.run'
		logging.info(" ")
		logging.info(" ")
		logging.info("******************************************************************************")
		logging.info("Begin " + func)
		
		keys = ["lockFilePath", "stagingWorkspace","productionWorkspace","bgbaseEditVersion","replica"]
		if not self._config.hasValues(keys):
			logging.error('Invalid config file.')
			logging.info('End ' + func);
			logging.info("******************************************************************************")
			return
			
		lockfile = util.LockFile(self._config['lockFilePath'])
		if lockfile.locked():
			logging.error("WarehouseToSde is already running")
			logging.info('If WarehouseToSde is not running, then delete the file %s', self._config['lockFilePath'])
			logging.info('End ' + func);
			logging.info("******************************************************************************")
			return
		lockfile.lock()
			
		num_changes = self._importChanges()
		if num_changes < 1:
			lockfile.unlock()
			if num_changes == 0:
				logging.info('There are no changes from Warehouse. SDE sync will not run')
			else:
				logging.info('Failed to refresh staging from Warehouse. SDE sync will not run')
			logging.info("End " + func)
			logging.info("******************************************************************************")
			return
			
		if self._reconcileStaging() == False:
			lockfile.unlock()
			logging.info('Failed to reconcile data in staging between versions. SDE sync will not run')
			logging.info("End " + func)
			logging.info("******************************************************************************")
			return
			
		if self._syncWithProd() == False:
			lockfile.unlock()
			logging.info('Failed to sync data between staging to production. SDE sync will not run')
			logging.info("End " + func)
			logging.info("******************************************************************************")
			return
		
		lockfile.unlock()
		logging.info("End " + func)
		logging.info("******************************************************************************")
		return
		
	def _importChanges(self):
		func = 'WarehouseToSde._importChanges'
		logging.info('Begin ' + func)
		bImport = False
		deleteList = dict()
		datasets = None
		num_total = 0
		try:
			datasets = self._warehouse.getSyncDatasets()
			num_updates = 0
			num_updates_total = 0
			num_inserts = 0
			num_inserts_total = 0
			num_deletes = 0
			num_deletes_total = 0
			num_records = 0
			for dataset in datasets:
				cursor = self._warehouse.getChanges(dataset)
				fields = self._dbutil.getColumns(cursor)
				if cursor is not None:
					logging.info("Begin iterating through change records")
					for row in cursor:
						operation = self._warehouse.getOperationType(row)
						bProcessed = False
						if operation == "insert":
							num_inserts_total = num_inserts_total + 1
							if self._processInserts(dataset, row, fields) == True:
								num_inserts = num_inserts + 1
								bProcessed = True
						elif operation == "update":
							num_updates_total = num_updates_total + 1
							if self._processUpdates(dataset, row, fields) == True:
								num_updates = num_updates + 1
								bProcessed = True
						elif operation == "delete":
							num_deletes_total = num_deletes_total + 1
							if self._processDeletes(dataset, row, fields) == True:
								num_deletes = num_deletes + 1
								bProcessed = True
						else:
							continue
							
						if bProcessed:
							if not (dataset['cdc_table'] in deleteList):
								deleteList[dataset['cdc_table']] = []
							deleteList[dataset['cdc_table']].append(row[fields['__$CDCKEY']])
						
						num_records = num_records + 1
					cursor.close()
					del cursor
					cursor = None
					
					num_total = num_inserts + num_updates + num_deletes
					logging.info("Processed " + str(num_total) + " out of " + str(num_records) + " database operations")
					logging.debug('Number of inserts: ' + str(num_inserts) + ' out of ' + str(num_inserts_total))
					logging.debug('Number of updates: ' + str(num_updates) + ' out of ' + str(num_updates_total))
					logging.debug('Number of deletes: ' + str(num_deletes) + ' out of ' + str(num_deletes_total))
					
					logging.info("End iterating through change records")
		except:
			num_total = -1
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			logging.error(msg)
		finally:
			if num_total > 0:
				self._warehouse.clearChanges(deleteList, datasets)
			logging.info('End ' + func)
		return num_total
			
	def _processInserts(self, dataset, row, fields):
		func = 'WarehouseToSde._processInserts'
		features = None
		feature = None
		bInsert = False
		try:
			key = row[fields[dataset['pkfield']]]
			layer = self._makeLayer(dataset, row, key)
			num_records = int(arcpy.GetCount_management(layer).getOutput(0))
			if num_records > 0:
				logging.error('Cannot insert record ' + str(key) + '. Record already exists')
			else:
				feature_class = os.path.join(self._stagingWorkspace(), dataset['table'])
				features = arcpy.InsertCursor(feature_class)
				field_names = self._getFieldNames(feature_class)
				feature = features.newRow()
				if self._loadFeature(feature, row, dataset, field_names, fields) == True:
					features.insertRow(feature)
					logging.debug('Successfully inserted record ' + str(key))
					bInsert = True
				else:
					logging.error("Insert failed for " + str(key) + ", could not load data for feature")
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(0)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		finally:
			if feature:
				del feature
			if features:
				del features
		return bInsert

	def _processUpdates(self, dataset, row, fields):
		func = 'WarehouseToSde._processUpdates'
		features = None
		feature = None
		bUpdate = False
		try:
			key = row[fields[dataset['pkfield']]]
			where_clause = dataset['pkfield'] + " = " + str(key)
			feature_class = os.path.join(self._stagingWorkspace(), dataset['table'])
			features = arcpy.UpdateCursor(feature_class, where_clause)
			field_names = self._getFieldNames(feature_class)
			
			num_features = 0
			for feature in features:
				num_features = num_features + 1
				if self._loadFeature(feature, row, dataset, field_names, fields) == True:
					features.updateRow(feature)
					logging.debug('Successfully updated record ' + str(key))
					bUpdate = True
				else:
					logging.error("Failed to load feature")
			
			if num_features == 0:
				logging.warn('Update cursor contained no features for ' + dataset['pkfield'] + ' = ' + str(key))
				logging.warn('Attempting to insert ' + str(key) + ' instead')
				if feature:
					del feature
					feature = None
				if features:
					del features
					features = None
				bUpdate = self._processInserts(dataset, row, fields)
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(0)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		finally:
			if feature:
				del feature
			if features:
				del features
		return bUpdate
		
	def _processDeletes(self, dataset, row, fields):
		func = 'WarehouseToSde._processDeletes'
		features = None
		feature = None
		bDelete= False
		try:
			key = row[fields[dataset['pkfield']]]
			where_clause = dataset['pkfield'] + " = " + str(key)
			feature_class = os.path.join(self._stagingWorkspace(), dataset['table'])
			features = arcpy.UpdateCursor(feature_class, where_clause)
			
			num_features = 0
			for feature in features:
				features.deleteRow(feature)
				num_features = num_features + 1
				logging.debug('Successfully deleted record ' + str(key))
				
			bDelete = num_features > 0
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(0)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		finally:
			if feature:
				del feature
			if features:
				del features
		return bDelete
			
	def _stagingWorkspace(self):
		return self._config['stagingWorkspace']
		
	def _productionWorkspace(self):
		return self._config['productionWorkspace']
		
	def _bgbaseEditVersion(self):
		return self._config['bgbaseEditVersion']
		
	def _replica(self):
		return self._config['replica']
		
	def _makeLayer(self, dataset, row, key):
		where_clause = dataset['pkfield'] + " = " + str(key)
		feature_class = os.path.join(self._stagingWorkspace(), dataset['table'])
		layer_name = "lyr" + str(uuid.uuid1()).replace("-", "")
		arcpy.MakeFeatureLayer_management(feature_class, layer_name, where_clause)
		arcpy.ChangeVersion_management(layer_name,'TRANSACTIONAL', self._bgbaseEditVersion(),'')
		return layer_name
		
	def _getFieldNames(self, feature_class):
		fields = arcpy.ListFields(feature_class)
		names = []
		for field in fields:
			if field.type == "OID" or field.type == "Geometry":
				continue
			names.append(field.name)
		return names
	
	def _loadFeature(self, feature, row, dataset, feature_fields, row_fields):
		try:
			for field_name in feature_fields:
				if row_fields.has_key(field_name):
					new_value = row[row_fields[field_name]]
					feature.setValue(field_name, new_value)
				elif field_name != "GlobalID":
					logging.warn(field_name + " not found in Warehouse")

			if dataset['xfield'] is not None and dataset['yfield'] is not None:
				x = row[row_fields[dataset['xfield']]]
				y = row[row_fields[dataset['yfield']]]
				if x is not None and y is not None:
					feature.shape = arcpy.PointGeometry(arcpy.Point(x, y))
			return True
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(0)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		return False
		
	def _reconcileStaging(self):
		func = 'WarehouseToSde.reconcile_staging'
		logging.info("Begin " + func)
		try:
			logging.debug("Reconciling data from staging BG-BASE to staging DEFAULT")
			arcpy.ReconcileVersions_management(self._stagingWorkspace(), "ALL_VERSIONS", "dbo.DEFAULT", self._bgbaseEditVersion(), "NO_LOCK_ACQUIRED", "NO_ABORT", "BY_OBJECT", "FAVOR_TARGET_VERSION", "POST", "KEEP_VERSION")
			logging.debug("Finished reconciling data from staging GIS to staging DEFAULT")
			
			logging.debug("Compressing data in Staging SDE")
			arcpy.Compress_management(self._stagingWorkspace())
			logging.debug("Finished compressing data in Staging SDE")
			return True
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(2)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End " + func)
		return False
		
	def _syncWithProd(self):
		func = '_syncWithProd'
		logging.info("Begin " + func)
		try:
			logging.debug("Synchronizing data from staging to production")
			arcpy.SynchronizeChanges_management(self._stagingWorkspace(), self._replica(), self._productionWorkspace(), "FROM_GEODATABASE1_TO_2", "IN_FAVOR_OF_GDB1", "BY_OBJECT", "DO_NOT_RECONCILE")
			logging.debug("Finished synchronizing data from production to staging")
			
			logging.debug("Compressing data in Production SDE")
			arcpy.Compress_management(self._productionWorkspace())
			logging.debug("Finished compressing data in Production SDE")
			return True
		except arcpy.ExecuteError:
			msgs = arcpy.GetMessages(2)
			arcpy.AddError(msgs)
			logging.error("ArcGIS error: %s", msgs)
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End " + func)
		return False

"""
DECLARE @begin_time datetime, @end_time datetime, @begin_lsn binary(10), @end_lsn binary(10);
SET @begin_time = '2000-01-01 00:00:00'
SET @end_time = '2020-01-01 00:00:00';
SELECT @begin_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than', @begin_time);
SELECT @end_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);
SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_PLANTS_LOCATION(@begin_lsn, @end_lsn, 'all');

select * from Warehouse.cdc.dbo_PLANTS_LOCATION_CT
where
CONVERT(VARCHAR(MAX), __$seqval, 2) in ('000002230000015D0003', '0000022B000006C80002', '0000022B000006E90002')

update PLANTS_LOCATION set GRID = 'TEST2.0'
where ACC_NUM = '10000' and line_seq = 1

SELECT * FROM Staging.dbo.PLANTS_LOCATION
where ACC_NUM = '10000' and line_seq = 1
order by SDE_STATE_ID desc

SELECT * FROM Staging.dbo.a67
where ACC_NUM = '10000' and line_seq = 1

INSERT INTO Warehouse.dbo.PLANTS_LOCATION(ACC_NUM,ACC_NUM_QUAL,X_COORD,Y_COORD,line_seq)
VALUES('-10000', 'A', 701987, 473261,1)

INSERT INTO Warehouse.dbo.PLANTS_LOCATION(ACC_NUM,ACC_NUM_QUAL,X_COORD,Y_COORD,line_seq)
VALUES('-10001', 'A', 703987, 475261,1)


select 0 as OBJECTID, ACC_NUM, rep_id, 'Warehouse' as SOURCE from Warehouse.dbo.PLANTS_LOCATION where ACC_NUM like '-1%'
union
select OBJECTID, ACC_NUM, rep_id, 'Staging' from Staging.dbo.PLANTS_LOCATION where ACC_NUM like '-1%'
union
select OBJECTID, ACC_NUM, rep_id, 'Staging Version' from Staging.dbo.a76 where ACC_NUM like '-1%'
union
select OBJECTID, ACC_NUM, rep_id, 'Production' from Production.dbo.PLANTS_LOCATION where ACC_NUM like '-1%'
union
select OBJECTID, ACC_NUM, rep_id, 'Production Version' from Production.dbo.a46 where ACC_NUM like '-1%'

"""