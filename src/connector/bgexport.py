import os, sys, arcpy
import traceback, logging, uuid
import arcpy
import util
from time import strftime

###################################################################################################
###################################################################################################
#
# class:	SdeToWarehouse
# purpose:	Worker class that creates an XML change file between SDE Staging and SDE Production.
#			This change file is sent to a configurable location, where BG-BASE will consume the
#			changes. This will allow edits made in SDE to make it into BG-BASE. If configured, the
#			class will also reconcile edits made in the Staging Edit Versions to the Staging
#			Default Version.
#
# author:	Jason Sardano
# date:		Aug 19 2013
#
###################################################################################################


class SdeToWarehouse(object):
	#config: connector.util.Config object with the following keys:
	#	tempPath:				Folder location where the script will store temp data.
	#	exportPath:				Folder location where the export file will be generated.
	#	deleteTempFiles:		true|false. If true, the script will delete the temp change files.
	#	autoReconcile			true|false. If true, the script will reconcile changes made in production edit with production default.
	#	stagingWorkspace:		Path to the Staging Workspace
	#	productionWorkspace:	Path to the Production Workspace
	#	stagingEditVersions:	Comma-delimited list of versions in Staging SDE to reconcile and post edits made in ArcGIS by users if autoReconcile is true
	#	replica:				Name of the the replica to sync.
	
	def __init__(self, config):
		self._config = config
		ts = strftime("%m%d%Y_%H%M%S")
		self._tempFile = self._tempPath() + '\\temp_' + ts + '.xml'
		self._exportFile = self._exportPath() + '\\changes_' + ts + '.xml'
		
	def run(self):
		func = 'SdeToWarehouse.run'
		logging.info(" ")
		logging.info(" ")
		logging.info("******************************************************************************")
		logging.info("Begin " + func)
		
		keys = ["tempPath", "exportPath", "deleteTempFiles", "autoReconcile", "stagingWorkspace","productionWorkspace","stagingEditVersions","replica"]
		if not self._config.hasValues(keys):
			logging.error('Invalid config file.')
			logging.info('End ' + func);
			logging.info("******************************************************************************")
			return
		
		if self._autoReconcile() == True:
			logging.info('Reconciling edits from edit versions to default in staging')
			self._reconcileStaging()
			
		logging.info("Exporting XML change file for BG-BASE")
		if self._exportChangeFile() == False:
			msg = 'Failed to create XML change file. Make sure that you have sufficient permissions in ' + self._tempPath()
			arcpy.AddError(msg)
			logging.error(msg)
			logging.error("Export change file failed. Sync will not run.")
			logging.info("******************************************************************************")
			return
	
		logging.info("Synchronizing changes in Staging Default SDE with Production SDE")
		if self._syncWithProd() == False:
			logging.error("Failed to sync with prod. Sync will not run.")
			logging.info("******************************************************************************")
			return False
	
		arcpy.AddMessage("Sending XML change file to BG-BASE folder queue")
		if self._sendChangeFile() == False:
			msg = 'Failed to copy XML change file to BG-BASE folder queue. Make sure that you have sufficient permissions in ' + self._exportPath()
			logging.error(msg)
			arcpy.AddError(msg)
			
		logging.info("End " + func)
		logging.info("******************************************************************************")
		return True
		
	def _reconcileStaging(self):
		func = '_reconcileStaging'
		logging.info("Begin " + func)
		
		try:
			logging.debug('Getting edits versions')
			versions = self._stagingEditVersions().split(',')
			if len(versions) > 0:
				logging.debug('Found ' + str(len(versions)) + ' edit versions to reconcile.')
				logging.debug("Reconciling data with Staging DEFAULT")
				arcpy.ReconcileVersions_management(self._stagingWorkspace(), "ALL_VERSIONS", "dbo.DEFAULT", ";".join(versions), "NO_LOCK_ACQUIRED", "NO_ABORT", "BY_OBJECT", "FAVOR_TARGET_VERSION", "POST", "KEEP_VERSION")
				logging.debug("Finished reconciling data with Staging DEFAULT")
			
			logging.debug("Compressing data in Staging SDE")
			arcpy.Compress_management(self._stagingWorkspace())
			logging.debug("Finished compressing data in Staging SDE")
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
		return
		
	def _exportChangeFile(self):
		func = '_exportChangeFile'
		result = False
		logging.info("Begin " + func)
		try:
			export_file = self._tempFile
			logging.debug("Exporting data change message to: %s", export_file)
			arcpy.ExportDataChangeMessage_management(self._stagingWorkspace(), export_file, self._replica(), "DO_NOT_SWITCH", "UNACKNOWLEDGED", "NEW_CHANGES")
			result = True
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
			
		logging.info("End export change file")
		return result
		
	def _syncWithProd(self):
		func = '_syncWithProd'
		logging.info("Begin " + func)
		try:
			logging.debug("Synchronizing data from production to staging")
			arcpy.SynchronizeChanges_management(self._stagingWorkspace(), self._replica(), self._productionWorkspace(), "FROM_GEODATABASE1_TO_2", "IN_FAVOR_OF_GDB1", "BY_OBJECT", "DO_NOT_RECONCILE")
			logging.debug("Finished synchronizing data from production to staging")
			
			logging.debug("Compressing data in Production SDE")
			arcpy.Compress_management(self._productionWorkspace())
			logging.debug("Finished compressing data in Production SDE")
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
		logging.info("End sync with staging")
		return
		
	def _sendChangeFile(self):
		result = False
		func = '_sendChangeFile'
		logging.info("Begin " + func)
		try:
			logging.debug('Copying %s to %s', self._tempFile, self._exportFile)
			self._copyFile(self._tempFile, self._exportFile)
			if self._deleteTempFiles():
				self._deleteFile(self._tempFile)
			result = True
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End "  + func)
		return result
		
	def _deleteFile(self, path_to_file):
		try:
			os.remove(path_to_file)
		except:
			#do nothing
			None
		return
	
	def _copyFile(self, source, dest):
		result = False
		func = '_copyFile'
		logging.info("Begin " + func)
		try:
			with open(source, 'r') as s:
				with open(dest, 'w') as d:
					line = s.readline()
					while line:
						d.write(line)
						line = s.readline()
			result = True
		except:
			tb = sys.exc_info()[2]
			tbinfo = traceback.format_tb(tb)[0]
			msg = "Error in " + func + ":\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
			arcpy.AddError(msg)
			logging.error(msg)
		logging.info("End " + func)
		return result
		
	def _autoReconcile(self):
		return self._config['autoReconcile'] == 'true'
		
	def _deleteTempFiles(self):
		return self._config['deleteTempFiles'] == 'true'
		
	def _tempPath(self):
		return self._config['tempPath']
		
	def _exportPath(self):
		return self._config['exportPath']
		
	def _stagingWorkspace(self):
		return self._config['stagingWorkspace']
		
	def _stagingEditVersions(self):
		return self._config['stagingEditVersions']
		
	def _productionWorkspace(self):
		return self._config['productionWorkspace']
		
	def _replica(self):
		return self._config['replica']
