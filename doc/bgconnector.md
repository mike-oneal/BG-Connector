BG-BASE Connector
================================

Background
----------

The BG-BASE Connector is a Python library that was written to synchronize data stored in BG-BASE and an ArcGIS 10.1 Enterprise Geodatabase.
The connector was written using Python 2.7, the Python 32-bit ODBC client and the ArcGIS 10.1 Python library.

Methodology
-----------

The database synchronization between BG-BASE and ArcGIS is achieved via multiple technologies. BG-BASE stores changes in various tables in a Microsoft SQL Server database (Warehouse). Change Data Capture (CDC) is enabled for tables that participate in synchronization. BG-BASE calls a batch file, which in turn calls a python file that launches the connector to initiate the BG-BASE to ArcGIS process (data import).

The connector reads a configuration table stored in the Warehouse database (described below) to determine which tables participate in the synchronization process. The connector calls CDC functions stored in the configuration table to read changes from BG-BASE, parses the changes, and stores the changes in the ArcGIS Geodatabase. The CDC function calls and parsing of the Warehouse data is accomplished via the Python ODBC client, and the data loading into the ArcGIS Geodatabase is accomplished via the ArcGIS Python library.

***Configuration Table (Warehouse.dbo.SDE_SYNC_TABLES)***
<table>
	<tr>
		<th>Column Name</th>
		<th>Description</th>
	</tr>
	<tr>
		<td>TABLE_NAME</td>
		<td>Name of the table in the ArcGIS database to receive the changes.</td>
	</tr>
	<tr>
		<td></td>
		<td>Name of the table in the SQL Server warehouse database.
		This can be a view.</td>
	</tr>
	<tr>
		<td>CDC_FUNCTION</td>
		<td>The name of the function that the script will call to read the changes from BG-BASE.</td>
	</tr>
	<tr>
		<td>PK_FIELD</td>
		<td>The primary key field name in the Warehouse table and ArcGIS table
		(must be the same in bothe databases).</td>
	</tr>
	<tr>
		<td>X_FIELD</td>
		<td>Optional. The field name that contains the Longitude data for the table.</td>
	</tr>
	<tr>
		<td>Y_FIELD</td>
		<td>Optional. The field name that contains the Latitude data for the table.</td>
	</tr>
	<tr>
		<td>DISABLED</td>
		<td>If set to 1, then the script will ignore this table.</td>
	</tr>
	<tr>
		<td>LAST_SYNC_DATE</td>
		<td>The last time this table was synchronized.</td>
	</tr>
</table>