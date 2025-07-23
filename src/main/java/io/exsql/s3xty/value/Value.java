package io.exsql.s3xty.value;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.UTF8String;

public interface Value {

    NullValue NULL_VALUE = new NullValue();

    default long toLong() {
        throw new UnsupportedOperationException("Cannot convert " + this.getClass().getSimpleName() + " to long");
    }

    default double toDouble() {
        throw new UnsupportedOperationException("Cannot convert " + this.getClass().getSimpleName() + " to double");
    }

    default boolean toBoolean() {
        throw new UnsupportedOperationException("Cannot convert " + this.getClass().getSimpleName() + " to boolean");
    }

    default long[] toLongs() {
        throw new UnsupportedOperationException("Cannot convert " + this.getClass().getSimpleName() + " to long");
    }

    default double[] toDoubles() {
        throw new UnsupportedOperationException("Cannot convert " + this.getClass().getSimpleName() + " to double");
    }

    default boolean[] toBooleans() {
        throw new UnsupportedOperationException("Cannot convert " + this.getClass().getSimpleName() + " to boolean");
    }

    default boolean isNull() {
        return this == NULL_VALUE;
    }

    static LongValue longValue(final String token) {
        return new LongValue(Long.parseLong(token));
    }

    static LongValue longValue(final long value) {
        return new LongValue(value);
    }

    static BooleanValue booleanValue(final String token) {
        return new BooleanValue(Boolean.parseBoolean(token));
    }

    static BooleanValue booleanValue(final boolean value) {
        return new BooleanValue(value);
    }

    static StringValue stringValue(final UTF8String token) {
        return new StringValue(token, false);
    }

    static RegexpValue regexpValue(final String pattern) {
        return new RegexpValue(com.google.re2j.Pattern.compile(pattern));
    }

    static FieldTypeValue fieldTypeValue(final DataType dataType) {
        return new FieldTypeValue(dataType);
    }

    static DoubleValue doubleValue(final double value) {
        return new DoubleValue(value);
    }

    static DoubleValue doubleValue(final String token) {
        return new DoubleValue(Double.parseDouble(token));
    }

    static NullValue nullValue() {
        return NULL_VALUE;
    }

    static StringArrayValue stringArrayValue(final UTF8String delimiter, final UTF8String wrapped) {
        return new StringArrayValue(wrapped.splitSQL(delimiter, 0));
    }

    static LongArrayValue longArrayValue(final UTF8String delimiter, final UTF8String wrapped) {
        return new LongArrayValue(new StringArrayValue(wrapped.splitSQL(delimiter, 0)).toLongs());
    }

    static DoubleArrayValue doubleArrayValue(final UTF8String delimiter, final UTF8String wrapped) {
        return new DoubleArrayValue(new StringArrayValue(wrapped.splitSQL(delimiter, 0)).toDoubles());
    }

    static BooleanArrayValue booleanArrayValue(final UTF8String delimiter, final UTF8String wrapped) {
        return new BooleanArrayValue(new StringArrayValue(wrapped.splitSQL(delimiter, 0)).toBooleans());
    }

}
