package io.exsql.s3xty;

import org.apache.spark.sql.types.DataType;

public record FieldTypeValue(DataType dataType) implements Value {
    @Override
    public String toString() {
        return String.format("fieldType(%s)", this.dataType.typeName());
    }
}
