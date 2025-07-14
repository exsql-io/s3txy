package io.exsql.s3xty;

public record DoubleValue(double wrapped) implements Value {
    @Override
    public double toDouble() {
        return this.wrapped;
    }

    @Override
    public boolean toBoolean() {
        return this.wrapped != 0;
    }

    @Override
    public String toString() {
        return String.format("double(%d)", this.wrapped);
    }
}
