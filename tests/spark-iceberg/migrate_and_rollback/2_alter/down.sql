-- Enable DROP COLUMN
ALTER TABLE bird_watch.flock SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','delta.minWriterVersion' = '5');
ALTER TABLE bird_watch.flock DROP COLUMN twigs_collected;