package io.exsql.s3xty.value;

import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public record StringArrayValue(UTF8String[] wrapped) implements Value {
    @Override
    public long[] toLongs() {
        var result = new long[this.wrapped.length];
        for (int i = 0; i < this.wrapped.length; i++) {
            result[i] = this.wrapped[i].toLongExact();
        }
        return result;
    }

    @Override
    public double[] toDoubles() {
        var result = new double[this.wrapped.length];
        for (int i = 0; i < this.wrapped.length; i++) {
            result[i] = Double.parseDouble(this.wrapped[i].toString());
        }
        return result;
    }

    @Override
    public boolean[] toBooleans() {
        var result = new boolean[this.wrapped.length];
        for (int i = 0; i < this.wrapped.length; i++) {
            result[i] = Boolean.parseBoolean(this.wrapped[i].toString());
        }
        return result;
    }

    @Override
    public @NotNull String toString() {
        return String.format("strings(%s)", Arrays.toString(this.wrapped));
    }
}
