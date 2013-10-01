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