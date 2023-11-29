package org.thesis.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface BeamOptions extends PipelineOptions {
    @Description("Input file to print.")
    @Validation.Required
    String getInputFile();
    void setInputFile(String value);

    @Description("Avro schema file.")
    @Validation.Required
    String getAvroSchemaPath();
    void setAvroSchemaPath(String value);

    @Description("Output path.")
    String getOutputPath();
    void setOutputPath(String value);

    @Description("Csv delimiter")
    String getCsvDelimiter();
    void setCsvDelimiter(String value);
}
