###################################################################################################
#
# script:  sde_to_warehouse.py
# purpose:	Creates an XML data change file that will be consumed by BG-BASE.
#			Synchronizes the data from Production SDE Default Version to Staging SDE Edit Version.
#			Reconciles the data in the Staging SDE Edit Version to Staging SDE Default Version
#
# author:	Jason Sardano
# date:		Aug 15, 2012
#
# notes:	This script was created from the process that is diagrammed in Database Schematic-dt.
#			Another script (warehouse_to_sde.py) needs to run in order to complete the
#			process outlined in the diagram AFTER the BG-BASE component has updated the
#			warehouse database.
#
# issues:	Reconcile to prod default only appears to work in ArcMap. May need to perform reconcile
#			and post during edit session? Sync with staging puts changes in BG-BASE version and 
#			Default versions. Edits do not move into DBO.GIS version. Need to look into this some
#			more.
#
###################################################################################################

import os, logging, logging.handlers
import arceditor, arcpy
from time import strftime

def get_timestamp():
	return strftime("_%m%d%Y_%H%M%S");

logfile_path = "\\\\arcgis.arbweb.harvard.edu\\Users\\Public\\Documents\\BG-BASE\\python_logs\\sde_to_warehouse.log"

base_path = "\\\\arcgis.arbweb.harvard.edu\\Users\\Public\\Documents\\ArcGIS\\";
production_workspace = base_path + "BG-BASE Production - DEFAULT Version.sde"
production_edit_workspace = base_path + "BG-BASE Production - GIS Edit Version.sde"
staging_workspace = base_path + "BG-BASE Staging - DEFAULT Version.sde"
staging_edit_workspace = base_path + "BG-BASE Staging - GIS Edit Version.sde"
staging_edit_version = "DBO.GIS"
prod_edit_version = "DBO.EDIT"
schema = "DBO.ProductionDEFAULTtoStagingGIS"

ts = get_timestamp()
bg_temp_path = base_path + "xmltemp\\";
bg_temp_file = bg_temp_path + "temp" + ts + ".xml"

bg_output_path = "\\\\bgbase.arbweb.harvard.edu\\BGBASE\\BGBASE6\\IMPORT\\"
bg_xml_output = bg_output_path + "changes" + ts + ".xml"
#bg_xml_output = "C:\\Python26\\ArcGIS10.0\\Scripts\\bg-sde-sync\\changes" + ts + ".xml"

# auto_reconcile_to_prod: Debug variable.
# If true, will reconcile edits made in production edit version
# with production default version
auto_reconcile_to_prod = True


def configure_logger():
	msg_format = "%(asctime)s %(levelname)s \t %(message)s";
	logging.basicConfig(level=logging.DEBUG, format=msg_format)
	handler = logging.handlers.TimedRotatingFileHandler(logfile_path, 'D', 1, 30)
	formatter = logging.Formatter(msg_format);
	handler.setFormatter(formatter)
	logging.getLogger('').addHandler(handler);
	return;

def delete_file(path_to_file):
	try:
		os.remove(path_to_file)
	except:
		#do nothing
		None
	return
	
def test():
	try:
		arcpy.AddMessage('Testing SDE')
		dataset = production_workspace + '\\dbo.PLANTS_LOCATION';
		fields = arcpy.ListFields(dataset);
		for field in fields:
			arcpy.AddMessage(field.name);
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	return;
	
def copy_file(source, dest):
	result = False;
	logging.info("Begin copy file")
	try:
		with open(source, 'r') as s:
			with open(dest, 'w') as d:
				line = s.readline()
				while line:
					d.write(line)
					line = s.readline()
		result = True;
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
		
	logging.info("End copy file")
	return result;
	
def export_change_file():
	result = False;
	logging.info("Begin export change file")
	try:
		
		logging.debug("Exporting data change message\n\tPath: %s", bg_temp_file)
		arcpy.ExportDataChangeMessage_management(production_workspace, bg_temp_file, schema, "DO_NOT_SWITCH", "UNACKNOWLEDGED", "NEW_CHANGES")
		result = True;
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
		
	logging.info("End export change file")
	return result;
	
def send_change_file():
	result = False;
	logging.info("Begin send change file")
	try:
		copy_file(bg_temp_file, bg_xml_output)
		#delete_file(bg_temp_file)
		result = True;
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(0)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
		
	logging.info("End send change file")
	return result;
	
	
def sync_with_staging():
	logging.info("Begin sync with staging")
	try:
		logging.debug("Synchronizing data from production to staging")
		arcpy.SynchronizeChanges_management(production_workspace, schema, staging_edit_workspace, "FROM_GEODATABASE1_TO_2", "IN_FAVOR_OF_GDB1", "BY_OBJECT", "DO_NOT_RECONCILE")
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
	
def reconcile_staging():
	logging.info("Begin reconcile staging")
	try:
		logging.debug("Reconciling data from staging GIS to staging DEFAULT")
		arcpy.ReconcileVersion_management(staging_workspace, staging_edit_version, "dbo.DEFAULT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "NO_LOCK_AQUIRED", "NO_ABORT", "POST")
		logging.debug("Finished reconciling data from staging GIS to staging DEFAULT")
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End reconcile staging")
	return;
	
def compress_staging():
	logging.info("Begin compress staging")
	try:
		logging.debug("Compressing data in Staging SDE Default")
		arcpy.Compress_management(staging_workspace)
		logging.debug("Finished compressing data in Staging SDE Default")
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End compress staging")
	return;	

#The data manager is responsible for reconciling edit to default in prod.
#This method is to assist in testing	
def reconcile_prod():
	logging.info("Begin reconcile prod")
	try:
		logging.debug("Reconciling data from Production Edit version to production DEFAULT")
		arcpy.ReconcileVersion_management(production_edit_workspace, prod_edit_version, "dbo.DEFAULT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "NO_LOCK_AQUIRED", "NO_ABORT", "POST")
		logging.debug("Finished reconciling data from Production Edit version to production DEFAULT")
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End reconcile prod")
	return;
	
def compress_prod():
	logging.info("Begin compress prod")
	try:
		logging.debug("Compressing data in Production SDE Default")
		arcpy.Compress_management(production_workspace)
		logging.debug("Finished compressing data in Production SDE Default")
	except arcpy.ExecuteError:
		msgs = arcpy.GetMessages(2)
		arcpy.AddError(msgs)
		logging.error("ArcGIS error: %s", msgs)
	except Exception as e:
		msg = e.message
		arcpy.AddError(msg)
		logging.error("Python error: %s", msg)
	logging.info("End compress prod")
	return;

def run():
	logging.debug("Begin sde_to_bg_import")
	arcpy.AddMessage("Running script sde_to_warehouse")
	
	if auto_reconcile_to_prod:
		logging.warn("********** Reconciling changes in prod from edit to default **********")
		logging.warn("********** Reconciling should be performed by data manager  **********")
		arcpy.AddMessage("Reconciling data from Production SDE Edit Version to Production SDE Default Version")
		reconcile_prod()

	arcpy.AddMessage("Exporting XML change file for BG-BASE")
	if export_change_file() == False:
		arcpy.AddError("Failed to create XML change file. Make sure that you have sufficeint permissions in " + bg_temp_path)
		logging.error("Export change file failed. Sync will not run.")
		return;
	
	arcpy.AddMessage("Synchronizing changes in Production Default SDE with Staging SDE")
	if sync_with_staging() == False:
		logging.error("Failed to sync with staging. Sync will not run.")
		return False;
	
	arcpy.AddMessage("Reconciling data from Staging SDE Edit Version to Staging SDE Default Version")
	reconcile_staging()
	
	arcpy.AddMessage("Compressing changes in Staging SDE")
	compress_staging()
	
	arcpy.AddMessage("Compressing changes in Production SDE")
	compress_prod()
	
	arcpy.AddMessage("Sending XML change file to BG-BASE folder queue")
	if send_change_file() == False:
		logging.error("Failed to copy XML change file to BG-BASE directory")
		arcpy.AddError("Failed to copy XML change file to BG-BASE folder queue. Make sure that you have sufficeint permissions in " + bg_output_path)
	else:
		arcpy.AddMessage("sde_to_warehouse script finished successfully")
	logging.info("End sde_to_bg_import")
	return True

if __name__ == "__main__":
	configure_logger()
	if run() == True:
		arcpy.SetParameterAsText(0, 'True')
	else:
		arcpy.SetParameterAsText(0, 'False')

