ALTER TABLE orders
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE orders
DROP COLUMN status;