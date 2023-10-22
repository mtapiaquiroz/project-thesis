package org.thesis.utils;

import org.apache.avro.Schema;
import org.apache.spark.sql.types.*;

public class AvroSchemaConverter {

    public static StructType avroSchemaToStructType(String avroSchema) {
        Schema.Parser parser = new Schema.Parser();
        Schema avro = parser.parse(avroSchema);
        return convertAvroToStructType(avro);
    }

    private static StructType convertAvroToStructType(Schema avroSchema) {
        StructType structType = new StructType();

        for (Schema.Field field : avroSchema.getFields()) {
            DataType dataType = convertAvroToSparkDataType(field.schema());
            structType = structType.add(field.name(), dataType, true);
        }

        return structType;
    }

    private static DataType convertAvroToSparkDataType(Schema avroType) {
        return switch (avroType.getType()) {
            case STRING -> DataTypes.StringType;
            case BOOLEAN -> DataTypes.BooleanType;
            case INT -> DataTypes.IntegerType;
            case LONG -> DataTypes.LongType;
            case FLOAT -> DataTypes.FloatType;
            case DOUBLE -> DataTypes.DoubleType;
            case ARRAY -> DataTypes.createArrayType(convertAvroToSparkDataType(avroType.getElementType()), true);
            case MAP ->
                    DataTypes.createMapType(DataTypes.StringType, convertAvroToSparkDataType(avroType.getValueType()));
            case UNION -> {
                for (Schema unionType : avroType.getTypes()) {
                    if (unionType.getType() != Schema.Type.NULL) {
                        yield convertAvroToSparkDataType(unionType);
                    }
                }
                yield DataTypes.NullType;
            }
            case RECORD -> convertAvroRecordToStructType(avroType);
            default -> DataTypes.NullType;
        };
    }

    private static StructType convertAvroRecordToStructType(Schema avroRecord) {
        StructType structType = new StructType();
        for (Schema.Field field : avroRecord.getFields()) {
            DataType dataType = convertAvroToSparkDataType(field.schema());
            structType = structType.add(field.name(), dataType, true);
        }
        return structType;
    }
}
