package io.exsql.s3xty;

import org.jetbrains.annotations.NotNull;

public record LongValue(long wrapped) implements Value {
    @Override
    public long toLong() {
        return this.wrapped;
    }

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
        return String.format("long(%d)", this.wrapped);
    }
}
