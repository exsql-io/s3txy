package io.exsql.s3xty;

import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;

public record FieldTypeValue(DataType dataType) implements Value {

    @Override
    public int compareTo(@NotNull final Value o) {
        if (o instanceof FieldTypeValue) {
            return this.dataType.typeName().compareTo(((FieldTypeValue) o).dataType.typeName());
        }

        return 0;
    }

    @Override
    public String toString() {
        return String.format("fieldType(%s)", this.dataType.typeName());
    }

}
