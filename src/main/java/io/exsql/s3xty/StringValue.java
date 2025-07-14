package io.exsql.s3xty;

import org.apache.spark.unsafe.types.UTF8String;

public record StringValue(UTF8String wrapped) implements Value {
    @Override
    public long toLong() {
        return this.wrapped.toLongExact();
    }

    @Override
    public double toDouble() {
        return Double.parseDouble(this.wrapped.toString());
    }

    @Override
    public boolean toBoolean() {
        return Boolean.parseBoolean(this.wrapped.toString());
    }

    @Override
    public String toString() {
        return String.format("string(%s)", this.wrapped);
    }
}
