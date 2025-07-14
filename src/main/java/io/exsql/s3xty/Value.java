package io.exsql.s3xty;

import org.apache.spark.sql.types.DataType;

public interface Value extends Comparable<Value> {

    static Value longValue(final String token) {
        return new LongValue(Long.parseLong(token));
    }

    static Value longValue(final long value) {
        return new LongValue(value);
    }

    static Value booleanValue(final String token) {
        return new BooleanValue(Boolean.parseBoolean(token));
    }

    static Value booleanValue(final boolean value) {
        return new BooleanValue(value);
    }

    static Value stringValue(final String token) {
        return new StringValue(token);
    }

    static Value fieldTypeValue(final DataType dataType) {
        return new FieldTypeValue(dataType);
    }

    static Value intValue(final int value) {
        return new IntValue(value);
    }

}
