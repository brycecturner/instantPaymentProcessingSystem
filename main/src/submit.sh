#! /bin/bash

$SPARK_HOME/bin/spark-submit \
  --packages io.delta:delta-core_2.12:2.3.0,io.delta:delta-storage:2.3.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  $1
