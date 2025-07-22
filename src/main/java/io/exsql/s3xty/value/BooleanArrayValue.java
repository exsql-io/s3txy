package io.exsql.s3xty.value;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public record BooleanArrayValue(boolean[] wrapped) implements Value {
    @Override
    public boolean[] toBooleans() {
        return this.wrapped;
    }

    @Override
    public @NotNull String toString() {
        return String.format("booleans(%s)", Arrays.toString(this.wrapped));
    }
}
