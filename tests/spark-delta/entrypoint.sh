#!/bin/bash
# Start Spark Connect Server
/opt/spark/bin/spark-submit \
    --class org.apache.spark.sql.connect.service.SparkConnectServer \
    --conf spark.connect.grpc.binding=0.0.0.0:15002 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --jars /opt/spark/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/jars/delta-storage-3.2.0.jar,/opt/spark/jars/spark-connect_2.12-3.5.7.jar \
    --name spark-connect-server
