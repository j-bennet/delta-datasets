#!/usr/bin/env bash

spark-submit \
--master "local[8]" \
  --deploy-mode client \
  --driver-memory 4g \
  --executor-memory 4g \
  --driver-java-options "-Droot.logger=ERROR,console" \
  --conf "spark.executor.memoryOverhead=2Gb" \
  --conf "spark.hadoop.delta.enableFastS3AListFrom=true" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  uber_lyft_delta.py