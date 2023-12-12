#!/bin/bash

for i in {1..10}
do
  echo "Execution $i:"
  gcloud dataproc jobs submit spark \
    --cluster thesis-spark-cluster-demo \
    --region us-central1 \
    --properties spark.jars.packages='org.apache.spark:spark-avro_2.12:3.3.2' \
    --class org.thesis.AppSpark \
    --jars gs://project-thesis-max/jars/apache-spark-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
    -- gs://project-thesis-max/input/us_accidents.csv \
       gs://project-thesis-max/input/us_accidents.avsc \
       gs://project-thesis-max/output/apache-spark/7m/$i
done