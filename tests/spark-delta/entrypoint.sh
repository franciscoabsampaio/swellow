#!/bin/bash
# Start Hive ThriftServer (runs on port 10000)
./sbin/start-thriftserver.sh \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# Test connection to ThriftServer
./bin/beeline -u jdbc:hive2://localhost:10000

# ./bin/spark-sql

/bin/bash