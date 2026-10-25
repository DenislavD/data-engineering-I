-- Snippets 1 from the Databricks tutorials

-- print(f"Username: {spark.sql('SELECT current_user()').first()[0]}")
-- print(f"Catalog:  {my_catalog}")
-- print(f"Schema:   {my_schema}")

Unity Catalog (UC)
Catalogs -> Schemas -> Tables/Volumes(raw)/Functions/AI

%sql
SELECT current_catalog(), current_schema();
SHOW TABLES;
SHOW VOLUMES;

-- spark.sql(f"LIST '/Volumes/{my_catalog}/{my_schema}/myfiles/'").display()
-- lists the files in the folder (Python)

SELECT * 
FROM read_files('/Volumes/' || my_catalog || '/' || my_schema || '/myfiles/employees.csv')
-- reads file/csv as table, creates a _rescued_data if column type infer fails

CTAS pattern: CREATE TABLE AS SELECT .. :

CREATE TABLE IF NOT EXISTS employees AS 
SELECT * 
FROM read_files('/Volumes/' || my_catalog || '/' || my_schema || '/myfiles/employees.csv');

DESCRIBE HISTORY employees;

/* Databricks' Delta format
The default data format in Databricks is it's proprietary Delta format.
It is a Parquet (binary, columnar, immutable) format + a transaction log (commit history).
Each ~10 commits Databricks (DB) OPMITIZES the Parquet files, consolidating them if needed.
and creates a current snapshot. Then from this snapshot only a few JSON commits are read to
reproduce the current data. This allows for time travel.

The operation is 2-step: Write new files as orphans, and add the new JSON log/commit as PUT
to the server that points to the new files.
*/
