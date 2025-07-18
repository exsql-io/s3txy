package io.exsql.s3xty;

import org.jetbrains.annotations.NotNull;

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
    public @NotNull String toString() {
        return String.format("double(%f)", this.wrapped);
    }
}
