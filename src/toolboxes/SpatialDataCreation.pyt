import arcpy

import uuid

g_temp_workspace = "scratch.gdb"

def guid():
	return str(uuid.uuid1()).replace('-','');
	
def get_count(dataset):
	return int(arcpy.GetCount_management(dataset).getOutput(0))

class Toolbox(object):
	def __init__(self):
		self.label = "Toolbox"
		self.alias = ""
		self.tools = [CreateFeatureClassFromTableTool]


class CreateFeatureClassFromTableTool(object):
	def __init__(self):
		self.label = "Create Feature Class From Table"
		self.description = "Creates a feature class or table in a geodatabase from external table"
		self.canRunInBackground = False

	def getParameterInfo(self):
		in_table = arcpy.Parameter(
			displayName="Input Table",
			name="in_table",
			datatype="DETable",
			parameterType="Required",
			direction="Input")
			
		in_workspace = arcpy.Parameter(
			displayName="Output Workspace",
			name="in_ws",
			datatype="DEWorkspace",
			parameterType="Required",
			direction="Input")
			
		in_name = arcpy.Parameter(
			displayName="Output Name",
			name="in_name",
			datatype="GPString",
			parameterType="Required",
			direction="Input")
			
		in_xfield = arcpy.Parameter(
			displayName="X Field",
			name="in_xfield",
			datatype="Field",
			parameterType="Optional",
			direction="Input")
		in_xfield.parameterDependencies = [in_table.name]
			
		in_yfield = arcpy.Parameter(
			displayName="Y Field",
			name="in_yfield",
			datatype="Field",
			parameterType="Optional",
			direction="Input")
		in_yfield.parameterDependencies = [in_table.name]
		
		in_sr = arcpy.Parameter(
			displayName="Spatial Reference",
			name="in_sr",
			datatype="GPSpatialReference",
			parameterType="Optional",
			direction="Input")
		in_sr.parameterDependencies = [in_xfield.name, in_yfield.name]
		in_sr.value = "PROJCS['NAD_1927_StatePlane_Massachusetts_Mainland_FIPS_2001',GEOGCS['GCS_North_American_1927',DATUM['D_North_American_1927',SPHEROID['Clarke_1866',6378206.4,294.9786982]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',600000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-71.5],PARAMETER['Standard_Parallel_1',41.71666666666667],PARAMETER['Standard_Parallel_2',42.68333333333333],PARAMETER['Latitude_Of_Origin',41.0],UNIT['Foot_US',0.3048006096012192]];-119901400 -96951900 3048.00609601219;0 1;0 1;3.28083333333333E-03;0.001;0.001;IsHighPrecision"
			
		out_dataset = arcpy.Parameter(
			displayName="New Dataset",
			name="out_dataset",
			datatype="DEDatasetType",
			parameterType="Derived",
			direction="Output")
			
		return [in_table, in_workspace, in_name, in_xfield, in_yfield, in_sr, out_dataset]

	def isLicensed(self):
		return True

	def updateParameters(self, parameters):
		return

	def updateMessages(self, parameters):
		return

	def execute(self, parameters, messages):
		in_table = parameters[0].valueAsText
		in_ws = parameters[1].valueAsText
		in_name = parameters[2].valueAsText
		in_x = parameters[3].valueAsText
		in_y = parameters[4].valueAsText
		in_sr = parameters[5].valueAsText
		
		datasets_to_delete = []
		temp_table_name = "tbl" + guid()
		arcpy.AddMessage("Importing " + in_table + " to temp workspace")
		arcpy.TableToTable_conversion(in_table, g_temp_workspace, temp_table_name)
		temp_table = g_temp_workspace + "\\" + temp_table_name
		datasets_to_delete.append(temp_table)
		arcpy.AddMessage("Adding GlobalID")
		arcpy.AddGlobalIDs_management([temp_table])
		
		output_dataset = in_ws + "\\" + in_name
		
		if in_x is not None and in_y is not None:
			xy_layer = "xy_" + guid()
			xy_dataset = g_temp_workspace + "\\" + xy_layer
			arcpy.AddMessage("Creating XY Event Layer")
			arcpy.MakeXYEventLayer_management(temp_table, in_x, in_y, xy_layer, in_sr)
			
			arcpy.AddMessage("Exporting XY layer to temp workspace")
			arcpy.FeatureClassToFeatureClass_conversion(xy_layer, g_temp_workspace, xy_layer, "", "", "")
			datasets_to_delete.append(xy_dataset)
			
			arcpy.AddMessage("Scrubbing points with invalid coordinates")
			temp_layer = "lyr_" + guid()
			where_clause = in_x + " IS NULL OR " + in_y + " IS NULL"
			arcpy.MakeFeatureLayer_management(xy_dataset, temp_layer, where_clause)
			num_invalid_points = get_count(temp_layer)
			arcpy.AddMessage("Found " + str(num_invalid_points) + " invalid points in layer")
			
			if num_invalid_points > 0:
				arcpy.AddMessage('Setting invalid points to null')
				#This seems to be the only way to null out the Shape
				arcpy.CalculateField_management(xy_dataset, "Shape", "Null", "VB", "")
				
				arcpy.AddMessage('Fixing valid points')
				temp_layer = "lyr" + guid()
				where_clause = in_x + " IS NOT NULL AND " + in_y + " IS NOT NULL"
				arcpy.MakeFeatureLayer_management(xy_dataset, temp_layer, where_clause)
				func = """
def getShape(x, y):
	return arcpy.Point(x, y)"""
				arcpy.CalculateField_management(temp_layer, "Shape", "getShape(!" + in_x + "!, !" + in_y + "!)", "PYTHON_9.3", func)
				
			arcpy.AddMessage("Exporting data to SDE")
			arcpy.FeatureClassToFeatureClass_conversion(temp_layer, in_ws, in_name)
			#For some reason, the Register as Versioned only works for feature classes if it's called twice.
			arcpy.RegisterAsVersioned_management(output_dataset, "NO_EDITS_TO_BASE")
		else:
			arcpy.AddMessage("Exporting data to SDE")
			arcpy.TableToTable_conversion(temp_table, in_ws, in_name)
			
			arcpy.AddMessage("Registering as versioned")
			arcpy.RegisterAsVersioned_management(output_dataset, "NO_EDITS_TO_BASE")
			
		arcpy.AddMessage("Cleaning up temp datasets")
		for dataset in datasets_to_delete:
			arcpy.Delete_management(dataset)
			
		parameters[6].value = output_dataset
		
		return
