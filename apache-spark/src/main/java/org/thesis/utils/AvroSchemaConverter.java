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
        switch (avroType.getType()) {
            case STRING:
                return DataTypes.StringType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case INT:
                return DataTypes.IntegerType;
            case LONG:
                return DataTypes.LongType;
            case FLOAT:
                return DataTypes.FloatType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case ARRAY:
                return DataTypes.createArrayType(convertAvroToSparkDataType(avroType.getElementType()), true);
            case MAP:
                return DataTypes.createMapType(DataTypes.StringType, convertAvroToSparkDataType(avroType.getValueType()));
            case UNION:
                for (Schema unionType : avroType.getTypes()) {
                    if (unionType.getType() != Schema.Type.NULL) {
                        return convertAvroToSparkDataType(unionType);
                    }
                }
                return DataTypes.NullType;
            case RECORD:
                return convertAvroRecordToStructType(avroType);
            default:
                return DataTypes.NullType;
        }
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
