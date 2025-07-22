package io.exsql.s3xty.value;

import org.jetbrains.annotations.NotNull;

public record BooleanValue(boolean wrapped) implements Value {
    @Override
    public long toLong() {
        return wrapped ? 1 : 0;
    }

    @Override
    public double toDouble() {
        return wrapped ? 1 : 0;
    }

    @Override
    public boolean toBoolean() {
        return this.wrapped;
    }

    @Override
    public @NotNull String toString() {
        return String.format("boolean(%s)", this.wrapped);
    }
}
