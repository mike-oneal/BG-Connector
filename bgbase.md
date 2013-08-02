Understanding BG-BASE structure
===============================

[BG-BASE](http://www.bg-base.com/), a botanical garden data management
system, works on top of the Revelation [OpenInsight][] database, which
has a [MultiValue](http://en.wikipedia.org/wiki/MultiValue) storage
model, where each ‘col x row’ data cell can store a list of
data. Unpacking this model into a standard cellular table can lead to
much repetition in the generated tables. In order to allow us to
access the data in the AA BG-BASE system, the designers of BG-BASE
have written scripts that dump the data in a standard Relational DB
(in this case MS SQL Server). We are now examining the resultant DB in
order to re-use the data in other downstream applications.  There is
no developers’ documentation for the BG-BASE table structure (but see
[here](http://www.bg-base.com/tables.htm)), so we are engaged in some
reverse engineering.  This is our understanding so far (see
[dot file](BGBASE_schema.dot)):

![Partial schema of BG-BASE, as dumped to RDBMS tables](../../../raw/master/doc/img/BGBASE_schema.jpg)

Primary keys
------------

Because of the nature of MultiValue DBs, we do not expect single field
unique keys in the dumped BG-BASE, but we may still be able to treat
some fields as keys. E.g.: 

 * `PLANTS.ACC_NUM_AND_QUAL` (only one duplicate: 876-70*C)
 * `ACCESSIONS.ACC_NUM` (no duplicates)
 * `NAMES.NAME_NUM` (no duplicates)

Check with, e.g.:

       SELECT TOP (10) NAME_NUM, COUNT(NAME_NUM) AS CNT 
     FROM NAMES GROUP BY NAME_NUM ORDER BY CNT DESC

Geographic data
---------------

Data in `PLANTS_LOCATION.X_COORD` and `PLANTS_LOCATION.Y_COORD` are
officially in the (NAD27, Mass State Plane Massachusetts Mainland FIPS
2001, in feet) coordinate system. This seems to be exactly the same as
EPSG:26786 (NAD27, Mass State Plane Massachusetts Mainland, in feet).

----

Accessing a MS SQL server via `fisql`
-------------------------------------

The MSSQL reference is [here](http://msdn.microsoft.com/en-us/library/ms189826(v=sql.90)).

      echo -e "use bgbw_aah\ngo\nselect top (5) * from names\ngo" | \
	    fisql -S XXX -U "XXX" -P XXX -s "|" | \
		sed -e 's/\ *|/|/g' -e 's/\ *$//g' -e '/^---/d'

      cat in.sql | fisql -S XXX -U "XXX" -P XXX -s "|" | \
	    sed -e 's/\ *|/|/g' -e 's/\ *$//g' -e '/^---/d' > out.csv
 
  [OpenInsight]: http://www.revelation.com/revelation.nsf/byTitle/5DA001116EB098C085256DC500658FF4?OpenDocument
  
  
