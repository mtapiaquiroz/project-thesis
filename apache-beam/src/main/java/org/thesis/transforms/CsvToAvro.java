package org.thesis.transforms;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thesis.options.BeamOptions;

import java.util.List;

public class CsvToAvro {
    public static PCollection<GenericRecord> runCsvToAvro(PCollection<String> rawData,
                                                          String delimiter, String schemaJson) {
        Schema schema = new Schema.Parser().parse(schemaJson);

        return rawData.apply("Convert CSV to Avro",
                        ParDo.of(new ConvertCsvToAvro(delimiter, schemaJson)))
                .setCoder(AvroCoder.of(GenericRecord.class, schema));
    }

    public static class ConvertCsvToAvro extends DoFn<String, GenericRecord> {
        private final String delimiter;
        private final String schemaJson;

        public ConvertCsvToAvro(String delimiter, String schemaJson) {
            this.delimiter = delimiter;
            this.schemaJson = schemaJson;
        }

        @ProcessElement
        public void processElement(ProcessContext ctx) {
            // Split CSV row using the specified delimiter
            String[] rowValues = ctx.element().split(delimiter);

            Schema schema = new Schema.Parser().parse(schemaJson);

            // Create Avro Generic Record
            GenericRecord genericRecord = new GenericData.Record(schema);

            List<Schema.Field> fields = genericRecord.getSchema().getFields();

            for (int index = 0; index < fields.size(); index++) {
                Schema.Field field = fields.get(index);
                String fieldType = getFieldType(field);

                if (rowValues.length <= index) {
                    // Handle missing values gracefully
                    genericRecord.put(field.name(), null);
                    continue;
                }

                String cellValue = rowValues[index].trim(); // Trim to handle leading/trailing spaces

                if (cellValue.isEmpty()) {
                    // Handle empty values by setting them to null
                    genericRecord.put(field.name(), null);
                    continue;
                }

                try {
                    switch (fieldType) {
                        case "string":
                            genericRecord.put(field.name(), cellValue);
                            break;
                        case "boolean":
                            genericRecord.put(field.name(), Boolean.valueOf(cellValue));
                            break;
                        case "int":
                            genericRecord.put(field.name(), Integer.valueOf(cellValue));
                            break;
                        case "long":
                            genericRecord.put(field.name(), Long.valueOf(cellValue));
                            break;
                        case "float":
                            genericRecord.put(field.name(), Float.valueOf(cellValue));
                            break;
                        case "double":
                            genericRecord.put(field.name(), Double.valueOf(cellValue));
                            break;
                        default:
                            // Handle unsupported data types
                            throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
                    }
                } catch (NumberFormatException e) {
                    // Handle invalid numeric values (non-numeric data)
                    genericRecord.put(field.name(), null);
                    // Log or handle the error as needed
                }
            }

            ctx.output(genericRecord);
        }
    }

    private static String getFieldType(Schema.Field field) {
        Schema.Type finalType = null;

        if (field.schema().getType() == Schema.Type.UNION) {
            for (Schema type : field.schema().getTypes()) {
                if (type.getType() != Schema.Type.NULL) {
                    finalType = type.getType();
                }
            }
            return finalType.toString().toLowerCase();
        } else {
            return field.schema().getType().toString().toLowerCase();
        }
    }
}
