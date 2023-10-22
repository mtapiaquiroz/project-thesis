package org.thesis;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.thesis.options.BeamOptions;
import org.thesis.transforms.CsvToAvro;
import org.thesis.utils.Utils;

import java.io.IOException;

public class AppBeam {

    public static void buildPipeline(Pipeline pipeline, BeamOptions options) throws IOException {
        String schemaJson = Utils.getSchema(options.getAvroSchemaPath());
        PCollection<String> rawData = pipeline.apply(TextIO.read().from(options.getInputFile()));
        PCollection<GenericRecord> avroData = CsvToAvro.runCsvToAvro(rawData, options.getCsvDelimiter(), schemaJson);
        avroData.apply(AvroIO.writeGenericRecords(schemaJson).to(options.getOutputPath() + "beam")
                .withSuffix(".avro").withCodec(CodecFactory.snappyCodec()));
    }


    public static void main(String[] args) throws IOException {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BeamOptions.class);
        var pipeline = Pipeline.create(options);
        long startTime = System.currentTimeMillis();
        AppBeam.buildPipeline(pipeline, options);
        pipeline.run().waitUntilFinish();
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("Time in sec: " + (executionTime/1000));
    }
}