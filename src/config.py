connector = {
	"importLogFile":r"[path]\logs\warehouse_to_sde.log",
	"exportLogFile":r"[path]\logs\sde_to_warehouse.log",
	"replicas":[
		{
			"name":"DBO.BGBASE_StagingToProduction",
			"disabled":False,
			"sqlServer": {
				"server":"[sqlserver]",
				"database":"Warehouse"
			},
			"tempPath":r"[path]\temp",
			"exportPath":r"[path]\NOT PROCESSED",
			"lockFilePath":r"[path]\StagingToProduction.loc",
			"deleteTempFiles":True,
			"autoReconcile":True,
			"stagingWorkspace":r"[path]\Staging.sde",
			"productionWorkspace":r"[path]\Production.sde",
			"sqlserverEditVersion":"DBO.BG-BASE",
			"stagingEditVersions":["DBO.DESKTOP","DBO.MOBILE"],
			"stagingDefaultVersion":"DBO.DEFAULT",
			"datasets":[
				{
					"cdcFunction":"cdc.fn_cdc_get_all_changes_dbo_PLANTS_LOCATION",
					"disabled":False,
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
				},
				{
					"cdcFunction":"cdc.fn_cdc_get_all_changes_dbo_PLANTS_CONDITION",
					"disabled":False,
					"sqlserverDataset":
					{
						"table":"Warehouse.cdc.dbo_PLANTS_CONDITION_CT",
						"primaryKey":"rep_id"
					},
					"sdeDataset":
					{
						"table":"Staging.dbo.PLANTS_CONDITION",
						"primaryKey":"rep_id"
					}
				},
				{
					"cdcFunction":"cdc.fn_cdc_get_all_changes_dbo_PLANTS_MEASURE_BY",
					"disabled":False,
					"sqlserverDataset":
					{
						"table":"Warehouse.cdc.dbo_PLANTS_MEASURE_BY_CT",
						"primaryKey":"rep_id"
					},
					"sdeDataset":
					{
						"table":"Staging.dbo.PLANTS_MEASURE_BY",
						"primaryKey":"rep_id"
					}
				},
				{
					"cdcFunction":"cdc.fn_cdc_get_all_changes_dbo_PLANTS_LABEL_HAVE_TYPE_BY",
					"disabled":True,
					"disabledReason":"PLANTS_LABEL_HAVE_TYPE_BY does not have CDC turned on.",
					"sqlserverDataset":
					{
						"table":"Warehouse.cdc.dbo_PLANTS_LABEL_HAVE_TYPE_CT",
						"primaryKey":"rep_id"
					},
					"sdeDataset":
					{
						"table":"Staging.dbo.PLANTS_MEASURE_BY",
						"primaryKey":"rep_id"
					}
				}
			]
		}
	]
};