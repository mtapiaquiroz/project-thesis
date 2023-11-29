#!/bin/bash

for i in {1..10}
do
  echo "Execution $i:"
  docker run --rm -it \
    -v /home/malkavian/Documentos/Thesis/code/project-thesis/apache-beam/target/apache-beam-1.0.0-SNAPSHOT-jar-with-dependencies.jar:/resources/beam.jar \
    -v /home/malkavian/Respaldo/thesis/data/us_accidents_700k.csv:/resources/us_accidents.csv \
    -v /home/malkavian/Documentos/Thesis/code/project-thesis/apache-spark/src/main/resources/us_accidents.avsc:/resources/schema.avsc \
    --entrypoint java \
    apache/beam_java11_sdk \
      -Xmx8g \
      -jar /resources/beam.jar \
      --inputFile=/resources/us_accidents.csv \
      --avroSchemaPath=/resources/schema.avsc \
      --outputPath=src/main/resources/output/ \
      --csvDelimiter=','
done
