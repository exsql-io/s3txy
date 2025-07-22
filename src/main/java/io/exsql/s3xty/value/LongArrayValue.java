package io.exsql.s3xty.value;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public record LongArrayValue(long[] wrapped) implements Value {
    @Override
    public long[] toLongs() {
        return this.wrapped;
    }

    @Override
    public @NotNull String toString() {
        return String.format("longs(%s)", Arrays.toString(this.wrapped));
    }
}
