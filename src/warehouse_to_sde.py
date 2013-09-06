###################################################################################################
###################################################################################################
#
# script:  warehouse_to_sde.py
# purpose:	Refreshes the data in Staging SDE BG-BASE Version from the Warehouse Database.
#			Reconciles the data in the Staging BG-BASE Version to Staging SDE Default Version
#			Synchronizes the data from Staging SDE Default Version to Production SDE Default Version.
#
# author:	Jason Sardano
# date:		Aug 15, 2012
#
# revisions:
#			Aug 31, 2012
#			Abandoned SQL procedure and PLANTS_LOCATION_MV in favor or processing CDC tables directly.
#			Used arcpy cursors instead to perform edits, this gets around processing the Add/Drop
#			tables in the geodatabase directly.
#
# notes:	This script was created from the process that is diagrammed in Database Schematic-dt.
#			Another script (sde_to_warehouse.py) needs to run in order to complete the
#			process outlined in the diagram BEFORE the BG-BASE component updates the warehouse database.
#
#			We should be using the CDC functions to get the changes from the warehouse rather than
#			parsing the warehouse tables directly. This means that we will also need to keep track
#			of the last sync date that the changes were processed.
#
###################################################################################################

import os, logging, logging.handlers
import arceditor, arcpy
import pyodbc
import time
import uuid
from datetime import datetime
from datetime import timedelta

logfile_path = "C:\\Users\\Public\\Documents\\BG-BASE\\python_logs\\warehouse_to_sde.log"

base_path = "C:\\Users\\Public\\Documents\\ArcGIS\\";
lockfile_path = base_path + "warehouse_to_sde.loc"

#SQL Server Settings
#This connection string will use the credentials of the logged in user
connection_string = "DRIVER={SQL Server};SERVER=arcgis.arbweb.harvard.edu;DATABASE=Warehouse;Trusted_Connection=yes"

#ArcGIS Settings
production_workspace = base_path + "BG-BASE Production - DEFAULT Version.sde"
production_edit_workspace = base_path + "BG-BASE Production - GIS Edit Version.sde";
staging_workspace = base_path + "BG-BASE Staging - DEFAULT Version.sde"
staging_edit_workspace = base_path + "BG-BASE Staging - BG-BASE Version.sde";
staging_gis_edit_workspace = base_path + "BG-BASE Staging - GIS Edit Version.sde";
schema = "DBO.StagingDEFAULTtoProductionDEFAULT"
prod_schema = "DBO.ProductionDEFAULTtoStagingGIS"
edit_version = "DBO.BG-BASE"
#edit_version = "DBO.GIS"
prod_edit_version = "DBO.EDIT"

spatial_ref = None
_datasets = None

primary_key = "rep_id"
x_field = "X_COORD"
y_field = "Y_COORD"

def configure_logger():
	msg_format = "%(asctime)s %(levelname)s \t %(message)s";
	logging.basicConfig(level=logging.DEBUG, format=msg_format)
	handler = logging.handlers.TimedRotatingFileHandler(logfile_path, 'D', 1, 30)
	formatter = logging.Formatter(msg_format);
	handler.setFormatter(formatter)
	logging.getLogger('').addHandler(handler);
	return;
	
def ends_with(s, c, last_char_only = True):
    if last_char_only:
        last_char = s[len(s) - 1:]
        if c == last_char:
            return True
        else:
            return False
    else:
        return s.rfind(c) > -1
        
def combine_path(path, name):
    if(not ends_with(path, "\\")):
        path = path + "\\"
    return path + name
	
def ts(prefix = ""):
	return prefix + str(int(time.mktime(datetime.now().timetuple())))
	
def get_cdc_table(name):
	return "Warehouse.cdc.dbo_" + name + "_CT"
	
def get_connection():
	try:
		logging.debug('Connecting to database')
		connection = pyodbc.connect(connection_string)
		return connection
	except Exception as e:
		logging.exception("Error connecting to database")
		return None;
		
def now_as_string():
	now = datetime.now()
	return now.strftime('%Y-%m-%d %H:%M:%S')
	
def tomorrow_as_string():
	tomorrow = datetime.now() + timedelta(days=1)
	return tomorrow.strftime('%Y-%m-%d %H:%M:%S')
	
def write_lockfile():
	try:
		with open(lockfile_path, 'w') as f:
			logging.info("Writing lock file.")
			f.write(now_as_string())
	except Exception as e:
		logging.error('Error writing lock file')
		logging.exception(e)
	return
	
def remove_lockfile():
	try:
		if os.path.exists(lockfile_path):
			logging.debug('Removing lockfile')
			os.remove(lockfile_path)
	except Exception as e:
		logging.error('Error removing lock file')
		logging.exception(e)
	return
		
def check_lockfile():
	try:
		if not os.path.exists(lockfile_path):
			write_lockfile()
			return True
		else:
			logging.debug('TODO: Parse time in lock file and compare to current time')
			return False
			#with open(lockfile_path, 'r') as f:
			#	line = f.readline()
			#	locktime = time.strptime(line, '%Y-%m-%d %H:%M:%S')
	except Exception as e:
		logging.error('Error checking lock file')
		logging.exception(e)
		return True

#This function caches an array of the table names to sync into memory.
#If we end up syncing hundreds ot tables, then the calling function
#should iterate over a cursor instead
def get_datasets(connection):
	logging.info("Begin get_datasets")
	
	datasets = []
	try:
		sql = 'SELECT * FROM dbo.SDE_SYNC_TABLES'
		cursor = connection.execute(sql)
		fields = get_columns(cursor)
		for row in cursor:
			dataset = dict()
			dataset['table'] = row[fields['TABLE_NAME']]
			dataset['func'] = row[fields['CDC_FUNCTION']]
			dataset['last_run'] = row[fields['LAST_SYNC_DATE']]
			dataset['cdc_table'] = row[fields['CDC_TABLE_NAME']]
			
			last_run = dataset['last_run']
			if last_run is None:
				dataset['last_run'] = '2000-01-01 00:00:00'
			else:
				str_last_run = last_run.strftime('%Y-%m-%d %H:%M:%S')
				dataset['last_run'] = str_last_run
			datasets.append(dataset)
		cursor.close()
			
	except Exception as e:
		logging.exception("Error getting datasets")

	logging.info("End get_datasets")
	global _datasets
	_datasets = datasets
	return datasets
	
def update_last_run(dataset, connection):
	logging.info("Begin update_last_run(" + dataset['table'] + '")')
	try:
		sql = 'UPDATE dbo.SDE_SYNC_TABLES set LAST_SYNC_DATE = SYSDATETIME() where TABLE_NAME = \'' + dataset['table'] + '\';COMMIT;'
		connection.execute(sql)
	except Exception as e:
		logging.exception("Error in update_last_run")

	logging.info("End update_last_run")
	return
	
def get_updated_columns(update_mask):
	#TODO
	#The SQL to get a list of updated columns based on the cdc update mask is
	#SELECT column_name
	#FROM [cdc].[captured_columns]
	#where
	#sys.fn_cdc_is_bit_set( [column_ordinal] ,0x03C000) = 1
	return []
	
def get_field_names(dataset):
	fields = arcpy.ListFields(dataset);
	names = []
	for field in fields:
		if field.type == "OID" or field.type == "Geometry":
			continue
		names.append(field.name)
	return names
	
def get_columns(cursor):
	cols = cursor.description;
	columns = dict()
	for i in xrange(len(cols)):
		field_name = cols[i][0];
		columns[field_name] = i
	return columns
	
def report_count(dataset, msg):
	try:
		count = int(arcpy.GetCount_management(dataset).getOutput(0))
		logging.info(dataset + ' - ' + msg + ': ' + str(count))
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	
def get_update_cursor(dataset, key):
	try:
		where_clause = primary_key + " = " + str(key)
		#feature_class = combine_path(staging_edit_workspace, dataset)
		feature_class = combine_path(staging_workspace, dataset)
		cursor = arcpy.UpdateCursor(feature_class, where_clause)
		return cursor
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	return None
	
def get_insert_cursor(dataset):
	try:
		#feature_class = combine_path(staging_edit_workspace, dataset)
		feature_class = combine_path(staging_workspace, dataset)
		cursor = arcpy.InsertCursor(feature_class)
		return cursor
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	return None
	
def create_spatial_reference(wkid):
	global spatial_ref
	if spatial_ref is None:
		spatial_ref = arcpy.SpatialReference();
		spatial_ref.factoryCode = wkid;
		spatial_ref.create();
	return sr;
	
def create_point(x, y):
	#wkid = 26786 #26786 = NAD_1927_StatePlane_Massachusetts_Mainland_FIPS_2001
	#sr = create_spatial_reference(wkid)
	
	point = arcpy.Point(x, y)
	geometry = arcpy.PointGeometry(point)
	return geometry
	
def load_feature(feature, row, feature_fields, row_fields):
	try:
		for field_name in feature_fields:
			if row_fields.has_key(field_name):
				new_value = row[row_fields[field_name]]
				feature.setValue(field_name, new_value)
			elif field_name != "GlobalID":
				logging.warn(field_name + " not found in Warehouse")
					
		x = row[row_fields[x_field]]
		y = row[row_fields[y_field]]
		if x is not None and y is not None:
			point = create_point(x, y)
			feature.shape = point
		return True
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	return False
	
def make_layer(dataset, row, fields):
	key = row[fields[primary_key]]
	where_clause = primary_key + " = " + str(key)
	#feature_class = combine_path(staging_edit_workspace, dataset)
	feature_class = combine_path(staging_workspace, dataset)
	layer_name = "lyr" + str(uuid.uuid1())
	arcpy.MakeFeatureLayer_management(feature_class, layer_name, where_clause)
	return layer_name
	
def process_deletes(dataset, row, fields):
	try:
		layer = make_layer(dataset, row, fields)
		arcpy.DeleteFeatures_management(layer)
		return True
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	return False
	
def process_inserts(dataset, row, fields):
	features = None
	feature = None
	bInsert = False
	try:
		key = row[fields[primary_key]]
		layer = make_layer(dataset, row, fields)
		num_records = int(arcpy.GetCount_management(layer).getOutput(0))
		if num_records > 0:
			logging.error('Cannot insert record ' + str(key) + '. Record already exists')
		else:
			features = get_insert_cursor(dataset)
			if features is None:
				logging.error("Failed to get insert cursor for " + dataset)
			else:
				#feature_class = combine_path(staging_edit_workspace, dataset)
				feature_class = combine_path(staging_workspace, dataset)
				field_names = get_field_names(feature_class)
				feature = features.newRow()
				if load_feature(feature, row, field_names, fields) == True:
					features.insertRow(feature)
					logging.debug('Successfully inserted record ' + str(key))
					bInsert = True
				else:
					logging.error("Insert failed for " + str(key) + ", could not load data for feature")
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	finally:
		if feature:
			del feature
		if features:
			del features
	return bInsert
	
def process_updates(dataset, row, fields):
	features = None
	feature = None
	bUpdate = False
	try:
		key = row[fields[primary_key]]
		features = get_update_cursor(dataset, key)
		if features is None:
			logging.error("Failed to get update cursor for " + dataset)
			return False
			
		#feature_class = combine_path(staging_edit_workspace, dataset)
		feature_class = combine_path(staging_workspace, dataset)
		field_names = get_field_names(feature_class)
		
		num_features = 0
		for feature in features:
			num_features = num_features + 1
			if load_feature(feature, row, field_names, fields) == True:
				features.updateRow(feature)
				bUpdate = True
			else:
				logging.error("Failed to load feature")
		
		if num_features == 0:
			logging.warn('Update cursor contained no features for ' + primary_key + ' = ' + str(key))
			logging.warn('Attempting to insert ' + str(key) + ' instead');
			if feature:
				del feature
				feature = None
			if features:
				del features
				features = None
			bUpdate = process_inserts(dataset, row, fields)
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	finally:
		if feature:
			del feature
		if features:
			del features
	return bUpdate

def import_warehouse_table_changes(dataset, connection):
	table = dataset['table']
	logging.info('Begin import_warehouse_table_changes for ' + table)
	result = False
	
	try:
		logging.info("Last run for " + table + ": " + str(dataset['last_run']));
		
		cursor = connection.cursor()
		sql = '''DECLARE @begin_time datetime, @end_time datetime, @begin_lsn binary(10), @end_lsn binary(10);
SET @begin_time = \'''' + dataset['last_run'] + '''\';
SET @end_time = \'''' + tomorrow_as_string() + '''\';
SELECT @begin_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than', @begin_time);
SELECT @end_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @end_time);
SELECT * FROM ''' + dataset['func'] + '''(@begin_lsn, @end_lsn, 'all');''' 
		cursor.execute(sql)
		run_date = now_as_string()
		
		fields = get_columns(cursor)
		num_updates = 0
		num_updates_total = 0
		num_inserts = 0
		num_inserts_total = 0
		num_deletes = 0
		num_deletes_total = 0
		num_records = 0
		for row in cursor:
			operation = row[fields["__$operation"]]
			if operation == 3:
				continue;
			num_records = num_records + 1
			if operation == 1:
				num_deletes_total = num_deletes_total + 1
				if process_deletes(table, row, fields) == True:
					num_deletes = num_deletes + 1
			elif operation == 2:
				num_inserts_total = num_inserts_total + 1
				if process_inserts(table, row, fields) == True:
					num_inserts = num_inserts + 1
			elif operation == 4:
				num_updates_total = num_updates_total + 1
				if process_updates(table, row, fields) == True:
					num_updates = num_updates + 1
					
		logging.info('Finished processing data')
		logging.info('Number of records: ' + str(num_records))
		logging.info('Number of inserts: ' + str(num_inserts) + ' out of ' + str(num_inserts_total))
		logging.info('Number of updates: ' + str(num_updates) + ' out of ' + str(num_updates_total))
		logging.info('Number of deletes: ' + str(num_deletes) + ' out of ' + str(num_deletes_total))
		
		num_changes = num_inserts + num_updates + num_deletes_total
		
		if num_changes > 0 or 1 > 0:
			#flushing CDC records
			try:
				logging.info('Flushing CDC records in ' + dataset['cdc_table'])
				sql = 'DELETE FROM ' + dataset['cdc_table'] + ' where __$start_lsn < sys.fn_cdc_map_time_to_lsn(\'largest less than or equal\', \'' + run_date + '\')';
				logging.info(sql)
				cursor.execute(sql)
			except Exception as e2:
				logging.warn('Error flushing CDC records')
				logging.warn(e2.message)
				
		cursor.close()
		update_last_run(dataset, connection)
		#DELETE FROM Warehouse.cdc.dbo_PLANTS_LOCATION_CT where __$start_lsn < sys.fn_cdc_map_time_to_lsn('largest less than or equal', '2012-11-28 20:38:23.603');
		
		return True

	except Exception as e:
		msg = str(e)
		arcpy.AddError(msg)
		logging.error("Error refreshing from warehouse: %s", msg)
	logging.info('End import_warehouse_table_changes')
	return False;
	
def import_warehouse_changes():
	logging.info('Begin import_warehouse_changes')
	connection = get_connection()
	if connection is None:
		logging.error("Failed to connect to warehouse, warehouse_to_sde will not run")
		return False

	result = False
	try:
		datasets = get_datasets(connection)
		for dataset in datasets:
			import_warehouse_table_changes(dataset, connection)
		result = True
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	finally:
		try:
			connection.close()
		except Exception as e:
			msg = e.message
			arcpy.AddError(msg)
			logging.error("Error closing connection: %s", msg)
	return result
			
def reconcile_staging():
	logging.info("Begin reconcile staging")
	try:
		logging.debug("Reconciling data from staging BG-BASE to staging DEFAULT")
		arcpy.ReconcileVersion_management(staging_edit_workspace, edit_version, "dbo.DEFAULT", "BY_OBJECT", "FAVOR_TARGET_VERSION", "NO_LOCK_AQUIRED", "NO_ABORT", "POST")
		logging.debug("Finished reconciling data from staging GIS to staging DEFAULT")
		return True
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End reconcile staging")
	return False;
	
def compress_staging():
	logging.info("Begin compress staging")
	try:
		logging.debug("Compressing data in Staging SDE BG-BASE")
		arcpy.Compress_management(staging_edit_workspace)
		logging.debug("Finished compressing data in Staging SDE BG-BASE")
		return True
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End compress staging")
	return False;
	
def sync_with_prod():
	logging.info("Begin sync with prod")
	try:
		logging.debug("Synchronizing data from staging to production")
		arcpy.SynchronizeChanges_management(staging_workspace, schema, production_workspace, "FROM_GEODATABASE1_TO_2", "IN_FAVOR_OF_GDB1", "BY_OBJECT", "DO_NOT_RECONCILE")
		logging.debug("Finished synchronizing data from staging to production")
		return True
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End sync with prod")
	return False;
	
def sync_with_staging():
	logging.info("Begin sync with staging")
	try:
		logging.debug("Synchronizing data from production to staging")
		arcpy.SynchronizeChanges_management(production_workspace, prod_schema, staging_gis_edit_workspace, "FROM_GEODATABASE1_TO_2", "IN_FAVOR_OF_GDB1", "BY_OBJECT", "DO_NOT_RECONCILE")
		logging.debug("Finished synchronizing data from production to staging")
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End sync with staging")
	return;
	
def reconcile_prod():
	logging.info("Begin reconcile prod")
	try:
		logging.debug("Reconciling data from Production Edit version to production DEFAULT")
		arcpy.ReconcileVersion_management(production_edit_workspace, prod_edit_version, "dbo.DEFAULT", "BY_OBJECT", "FAVOR_TARGET_VERSION", "NO_LOCK_AQUIRED", "NO_ABORT", "POST")
		logging.debug("Finished reconciling data from Production Edit version to production DEFAULT")
		return True
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End reconcile prod")
	return False;
	
def compress_prod():
	logging.info("Begin compress prod")
	try:
		logging.debug("Compressing data in Production SDE Edit Version")
		arcpy.Compress_management(production_edit_workspace)
		logging.debug("Finished compressing data in Production SDE Edit Version")
		return True
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End compress prod")
	return False;
	
def debug_dataset_counts(workspace, msg):
	if _datasets is None:
		return
		
	for dataset in _datasets:
		report_count(workspace + '\\' + dataset['table'], msg)
	return
	
def debug_switch_datasets(from_dataset, to_dataset):
	global _datasets
	#cheating here for debugging purposes
	for dataset in _datasets:
		table = dataset['table']
		if table.startswith((from_dataset + '.')) == True:
			table = table.replace(from_dataset + '.', to_dataset + '.', 1)
			dataset['table'] = table
	return
	
def run():
	logging.info(" ");
	logging.info(" ");
	logging.info("******************************************************************************");
	logging.info("Begin warehouse_to_sde")
	
	if check_lockfile() == False:
		logging.warn("Lock file (" + lockfile_path + ") exists. Cannot run")
		logging.info("End warehouse_to_sde")
		return

	try:
		global _datasets;
		_datasets = get_datasets(get_connection())
	except Exception as e:
		logging.error('Failed to load datasets for count debugging')
		logging.exception(e);
	
	debug_dataset_counts(staging_workspace, 'Staging counts before warehouse import')
	if import_warehouse_changes() == False:
		remove_lockfile()
		logging.info('Failed to refresh staging from Warehouse. SDE sync will not run')
		logging.info(" ");
		logging.info(" ");
		logging.info("End warehouse_to_sde")
		logging.info("******************************************************************************");
		return;
	debug_dataset_counts(staging_workspace, 'Staging counts after warehouse import')
	
	debug_dataset_counts(staging_workspace, 'Staging counts before reconcile')
	if reconcile_staging() == False:
		remove_lockfile()
		logging.info('Failed to reconcile data in staging between versions. SDE sync will not run')
		logging.info(" ");
		logging.info(" ");
		logging.info("End warehouse_to_sde")
		logging.info("***");
		return;
	debug_dataset_counts(staging_workspace, 'Staging counts after reconcile')
	
	#cheating here for debugging purposes
	debug_switch_datasets('Staging', 'Production')
	
	debug_dataset_counts(production_workspace, 'Production counts before sync with prod')
	if sync_with_prod() == False:
		remove_lockfile()
		logging.info('Failed to sync data between staging to production. SDE sync will not run')
		logging.info(" ");
		logging.info(" ");
		logging.info("End warehouse_to_sde")
		logging.info("***");
		return;
	debug_dataset_counts(production_workspace, 'Production counts after sync with prod')
	
	debug_switch_datasets('Production', 'Staging')
	debug_dataset_counts(staging_workspace, 'Staging counts before compress')
	compress_staging()
	debug_dataset_counts(staging_workspace, 'Staging counts after compress')
	
	debug_switch_datasets('Staging', 'Production')
	debug_dataset_counts(production_workspace, 'Production counts before reconcile')
	reconcile_prod()
	debug_dataset_counts(production_workspace, 'Production counts after reconcile')
	
	#Flush the changes out of prod so they don't make it back to BG-BASE.
	sync_with_staging()
	
	debug_dataset_counts(production_workspace, 'Production counts before compress')
	compress_prod()
	debug_dataset_counts(production_workspace, 'Production counts after compress')
		
	logging.info(" ");
	logging.info(" ");
	logging.info("End warehouse_to_sde")
	logging.info("***");
	
	remove_lockfile()
	
	return

if __name__ == "__main__":
	configure_logger()
	run()
