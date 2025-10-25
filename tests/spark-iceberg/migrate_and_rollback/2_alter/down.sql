-- Enable DROP COLUMN
ALTER TABLE flock SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','delta.minWriterVersion' = '5');
ALTER TABLE flock DROP COLUMN twigs_collected;