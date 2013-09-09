BG-BASE Connector
================================

Background
----------

The BG-BASE Connector is a Python library that was written to synchronize data stored in BG-BASE and an ArcGIS 10.1 Enterprise Geodatabase.
The connector was written using Python 2.7, the Python 32-bit ODBC client and the ArcGIS 10.1 Python library.

Methodology
-----------

The database synchronization between BG-BASE and ArcGIS is achieved via multiple technologies. BG-BASE stores changes in various tables in a Microsoft SQL Server database (Warehouse). Change Data Capture (CDC) is enabled for tables that participate in synchronization. BG-BASE calls a batch file, which in turn calls a python file that launches the connector to initiate the BG-BASE to ArcGIS process (data import).

The connector reads a table stored in the Warehouse database (described below) to determine which tables participate in the synchronization process.