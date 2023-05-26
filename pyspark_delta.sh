#!/usr/bin/env bash

pyspark \
--conf "spark.hadoop.delta.enableFastS3AListFrom=true" \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"