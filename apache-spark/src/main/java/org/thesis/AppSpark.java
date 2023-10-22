package org.thesis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.thesis.utils.AvroSchemaConverter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
public class AppSpark {

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: AppSpark <inputCsvPath> <avroSchemaPath> <outputAvroPath>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("AppSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        SparkSession spark = SparkSession.builder().appName("AppSpark").config(sparkConf).getOrCreate();
        long startTime = System.currentTimeMillis();

        String inputCsvPath = args[0];
        String avroSchemaPath = args[1];
        String outputAvroPath = args[2];

        String avroSchemaString = new String(Files.readAllBytes(Paths.get(avroSchemaPath)));
        StructType structType = AvroSchemaConverter.avroSchemaToStructType(avroSchemaString);

        Dataset<Row> csvData = sqlContext.read().
                format("com.databricks.spark.csv")
                .schema(structType)
                .option("header", "false")
                .csv(inputCsvPath);

        csvData.write()
                .mode(SaveMode.Overwrite)
                .format("com.databricks.spark.avro")
                .save(outputAvroPath);

        spark.stop();
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("Time in sec: " + (executionTime/1000));

    }
}