BG-BASE Connector
================================

Background
----------
The BG-BASE Connector is a Python library that was written to synchronize data stored in BG-BASE and an ArcGIS 10.1 Enterprise Geodatabase.
The connector was written using Python 2.7, the Python 32-bit ODBC client and the ArcGIS 10.1 Python library.

Methodology
-----------
*Data Import*
The database synchronization between BG-BASE and ArcGIS is achieved via multiple technologies. BG-BASE stores changes in various tables in a Microsoft SQL Server database (Warehouse). Change Data Capture (CDC) is enabled for the Warehouse tables that participate in synchronization. BG-BASE calls a batch file, which in turn calls a python file that launches the connector to initiate the BG-BASE to ArcGIS process (data import).

The connector reads a configuration Python file (described below) to determine which replicas and tables participate in the synchronization process. The connector calls CDC functions referenced in the config file to read changes from BG-BASE, parses the changes, and stores the changes in an ArcGIS Geodatabase. The CDC function calls and parsing of the Warehouse data is accomplished via the Python ODBC client, and the data loading into the ArcGIS Geodatabase is accomplished via the ArcGIS Python library.

***Configuration File (config.py)***

The configuration file is a python file that has a required Python dictionary named "connector". An explanation of the config properties is described after the Python file below. The python file looks like:
<code>
connector = {
	"importLogFile":r"C:\Temp\SqlServerConnector\logs\warehouse_to_sde.log",
	"exportLogFile":r"C:\Temp\SqlServerConnector\logs\sde_to_warehouse.log",
	"replicas":[
		{
			"name":"DBO.BGBASE_StagingToProduction",
			"disabled":False,
			"sqlServer": {
				"server":"localhost",
				"database":"Warehouse"
			},
			"tempPath":r"C:\Temp\SqlServerConnector\temp",
			"exportPath":r"C:\Temp\SqlServerConnector\temp",
			"lockFilePath":r"C:\Temp\SqlServerConnector\temp\StagingToProduction.loc",
			"deleteTempFiles":True,
			"autoReconcile":True,
			"stagingWorkspace":r"C:\Temp\SDE Connections\Staging@ARCGIS10.sde",
			"productionWorkspace":r"C:\Temp\SDE Connections\Production@ARCGIS10.sde",
			"sqlserverEditVersion":"DBO.BG-BASE",
			"stagingEditVersions":["DBO.DESKTOP","DBO.MOBILE"],
			"stagingDefaultVersion":"DBO.DEFAULT",
			"datasets":[
				{
					"cdcFunction":"cdc.fn_cdc_get_all_changes_dbo_PLANTS_LOCATION",
					"sqlserverDataset":
					{
						"table":"Warehouse.cdc.dbo_PLANTS_LOCATION_CT",
						"primaryKey":"rep_id",
						"xField":"X_COORD",
						"yField":"Y_COORD"
					},
					"sdeDataset":
					{
						"table":"Staging.dbo.PLANTS_LOCATION",
						"primaryKey":"rep_id"
					}
				}
			]
		}
	]
};
</code>

Explanation of the config file:

connector = {
	"importLogFile": Path to the log file for the import process,
	"exportLogFile": Path to the log file for the export process,
	"replicas":[ #replicas is an array of replica configurations.  1 or more objects are required.
		{
			"name":Name of the Geodatabase replica,
			"disabled":If True, then the connector will ignore this replica,
			"sqlServer": { #dictionary describing SQL Server connection properties
				"server":Name of the SQL Server,
				"database":Database name of the database to connect to
			},
			"tempPath": Path where the connector writes its temporary data to,
			"exportPath": Path where the connector writes data changes from Staging SDE to Production SDE,
			"lockFilePath": File where the connector creates a lock file for processing,
			"deleteTempFiles": If True the connector will delete the temp files that it writes out in the tempPath,
			"autoReconcile": If True, the connector will reconcile data updates in Staging and Production SDE,
			"stagingWorkspace": SDE Connection file for the Staging Geodatabase,
			"productionWorkspace": SDE Connection file for the Production Geodatabase,
			"sqlserverEditVersion": SDE Version name where CDC changes are written to,
			"stagingEditVersions":[An array SDE Version names of Versions that are to be reconciled during the export process. Required if autoReconcile is True],
			"stagingDefaultVersion":SDE Version name of the Default instance in the Staging SDE,
			"datasets":[ #An array of Dataset configuration objects. 1 or more objects are required.
				{
					"cdcFunction": The CDC function to execute to get the changes from the Warehouse,
					"sqlserverDataset": #Python dictionary describing the dataset in the Warehouse to synchronize
					{
						"table":Name of the CDC change table in Warehouse (not the actual table name),
						"primaryKey": The primary key field for the dataset,
						"xField": Optional, the field name of the Longitude Field,
						"yField":Optional, the field name of the Latitude Field (If the X or Y field aren't specified, then the connector will treat the dataset as a table instead of a feature class.
					},
					"sdeDataset": #Python dictionary describing the dataset in SDE to synchronize.
					{
						"table": Name of the table or feature class in SDE,
						"primaryKey": The primary key field for the dataset,
					}
				}
			]
		}
	]
};

*Data Export*
The database synchronization between ArcGIS and BG-BASE is achieved using ArcGIS geodatabase replicas. An ArcGIS user launches a batch file that calls the connector data export routine. The connector uses ArcGIS geoprocessing tools to create an XML data change file, which contains data changes that have been made by users using ArcGIS software, and then exports the XML change file to a location where BG-BASE reads the XML data change file. BG-BASE then applies the data changes to the BG-BASE tables.

Geodatabase Design
------------------
*Databases*
The enterprise geodatabase contains 2 databases named Staging and Production. A geodatabase replica named StagingToProduction sits in between the 2 geodatabases, and is used to synchronize the data between Staging and Production. The replica is a one-way replica, with the Staging database as the parent and the Production database as a child to Staging. In this scenario, the Production database is a clone of the Staging database, with users making their edits in Staging and Profuction serving as a resd-only database.

*Versions*
There are 3 versions in the Staging geodatabase besides the Default database: BG-BASE, Desktop and Mobile. The BG-BASE version is where the connector writes edits from the CDC data from the Warehouse database. The Desktop and Mobile versions store edits made by users and are used in the data export process.

Visual representation of geodatabase design:
![gdb](gdb.png "Geodatabase Design")

Spatial Data Creation
---------------------
A custom ArcGIS Python Toolbox was written to create the spatial data from tables stored in the Warehouse database. At the beginning of the BG-BASE Connector, there was only one spatial dataset, Plants_Location, so an import tool was not necessary. However, as the BG-BASE Connector has evolved and the Arboretum's GIS capabilities have increased, an import tool was necessary.

The import tool allows a user to specify a source table to import the data from, an output location where the data will be created, a name for the exported dataset, and optional X and Y field, and an optional sptial reference. If the X and Y fields are specified, then the dataset will be imported as a spatially-enabled table (feature class). Otherwise, the dataset will be imported as a simple table. If a feature class is imported, then the points are created using an XY Event Layer.

Geodatabase Replicas
--------------------
As stated earlier, the BG-BASE Connector uses Geodatabase Replicas to synchronize data between Staging and Production, and to send data changes to BG-BASE. As of ArcGIS 10.1, datasets can only be added to a replica during the replica's creation process; it is not possible to add an additional dataset to a replica once the replica has been created without custom code written in ArcObjects.

Python Code
-----------
The connector is made of several Python classes, with 2 launcher files sitting at the top of the package structure.

* connector.db: Module that contains classes that read the config file and return properties of the replicas
	*Replicas: Class that contains an array of replicas
	*Replica: Class that stores information about the replica, and contains an array of dataset objects.
	*Dataset: Class that stored information about the dataset to replicate.

* connector.util: Module that contains classes that perform various utility functions
	* DateUtil: Class that performs various date/string operations.
	* DBUtil: Class that performs various operations against the Python ODBC client record sets.
	* LockFile: Class that writes out a lock file during the duration of the data import routine.
* connector.io: Module that contains worker classes that import, export, synchronize and replicate data.
	* SqlServerImporter: Class that calls the CDC functions to read the data changes from BG-BASE and imports the changes into ArcGIS.
	* GeodatabaseExporter: Class that calls ArcGIS geoprocessing tools to create the XML data change files fot BG-BASE to consume.
* top level:
	* sqlserver_to_sde: Script that creates an intance of connector.io.SqlServerImporter and calls the class' run method.
	* sde_to_xml: Script that creates an intance of connector.io.GeodatabaseExporter and calls the class' run method.


Flow Charts
-------------
*Data Import*
![dataimport](data_import_flowchart.png "Data Import Flow Chart")

*Data Export*
![dataexport](data_export_flowchart.png "Data Export Flow Chart")

General Thoughts and Concerns
-----------------------------
*System Design*
The BG-BASE Connector is a loosely-coupled system. It relies on BG-BASE calling a Python module via a batch file in order to import the data from BG-BASE to the geodatabase. BG-BASE is responsible for deciding when to call the script, and must have read access to the script. The export process can be launched manually by a user or from a scheduled task.

*Data Integrity*
Because the BG-BASE Connector is a loosely-coupled system, it is possible that the data between BG-BASE and the geodatabase will get out of sync. This can happen when CDC is not enabled in the Warehouse, when the XML change files of geodatabase changes are not processed correctly, or if there is a general network I/O error during the script's execution process.  This then requires a re-synchronization between the two systems.

*Performance*
The desired synchronization between Warehouse and the geodatabase can be as close to real time as possible. The BG-BASE Connector in its current form experiences a lot of overhead. The current implementation is run on a scheduled interval rather than a transactional model to reduce the overhead.

*BG-BASE Implementation*
BG-BASE records observations using a concept of a line sequence, where a value of 1 is the most recent observation, the value of 2 is the second most recent observation, etc. When a new observation is recorded for a plant, there are X number of database transactions (and potentially X + 1, depending on how BG-BASE inserts the data).

So if a plant has 10 observations, and a new observation is made, then the following pseudo code is executed:
*Insert into Plants_X(id, line_seq) Values('fakeid', 0)*
*Update Plants_IX set line_seq =  line_seq + 10*

These 2 SQL statements will generate 12 CDC records, one for the insert, and 11 for the line_seq updates.

Rather than using a line_seq to track the most recent observation, a FROM_DATE and TO_DATE field can be added, with a null TO_DATE value representing the most recent observation. For example:
*Update Plants_X set to_date = now() where to_date is null*
*Insert into Plants_X(id, to_date) Values('fakeid', null)*

*An alternate approach*
BG-BASE may be able to use ArcObjects to interact with the Geodatabase directly, therefore, the BG-BASE connector model can altogether be removed. BG-BASE could apply changes directly to Staging and call the synchronization process immediately. The export from the geodatabase to BG-BASE will still require a tool to generate the change file for BG-BASE to read.  To be further explored.

An alternate approach to ArcOvhects is using an ArcGIS Server Feature Service, and BG-BASE calling the feature service's REST endpoint with HTTP requests. This eliminates the complexity of ArcObjects, but introduces the overhead of maintaining ArcGIS Server.  To be further explored.
