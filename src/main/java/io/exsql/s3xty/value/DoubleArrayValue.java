package io.exsql.s3xty.value;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public record DoubleArrayValue(double[] wrapped) implements Value {
    @Override
    public double[] toDoubles() {
        return this.wrapped;
    }

    @Override
    public @NotNull String toString() {
        return String.format("doubles(%s)", Arrays.toString(this.wrapped));
    }
}
