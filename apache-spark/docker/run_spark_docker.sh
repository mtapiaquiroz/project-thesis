#!/bin/bash

for i in {1..10}
do
  echo "Execution $i:"
  docker run --rm -it \
    -v /home/malkavian/Documentos/Thesis/code/project-thesis/apache-spark/target/apache-spark-1.0.0-SNAPSHOT-jar-with-dependencies.jar:/resources/spark.jar \
    -v /home/malkavian/Respaldo/thesis/data/us_accidents.csv:/resources/us_accidents.csv \
    -v /home/malkavian/Documentos/Thesis/code/project-thesis/apache-spark/src/main/resources/us_accidents.avsc:/resources/schema.avsc \
    --entrypoint /opt/spark/bin/spark-submit \
    apache/spark:3.3.3-scala2.12-java11-python3-r-ubuntu \
    --class org.thesis.AppSpark \
    --packages org.apache.spark:spark-avro_2.12:3.3.2 \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    --master local[*] \
    /resources/spark.jar \
    /resources/us_accidents.csv \
    /resources/schema.avsc \
    /opt/spark/work-dir/output/
done