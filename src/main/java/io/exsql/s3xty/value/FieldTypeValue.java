package io.exsql.s3xty.value;

import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;

public record FieldTypeValue(DataType dataType) implements Value {
    @Override
    public @NotNull String toString() {
        return String.format("fieldType(%s)", this.dataType.typeName());
    }
}
