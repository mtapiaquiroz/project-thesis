#!/bin/bash
cd ..
for i in {1..10}
do
  echo "Execution $i:"
  mvn -Pdataflow compile exec:java \
    -Dexec.mainClass=org.thesis.AppBeam \
    -Dexec.args="--project=buoyant-voyage-402900 \
    --gcpTempLocation=gs://project-thesis-max/temp/ \
    --runner=DataflowRunner \
    --region=us-central1 \
    --inputFile=gs://project-thesis-max/input/us_accidents_70k.csv \
    --avroSchemaPath=gs://project-thesis-max/input/us_accidents.avsc \
    --outputPath=gs://project-thesis-max/output/apache-beam/7k/ \
    --csvDelimiter=','"
done
