package io.exsql.s3xty;

import org.jetbrains.annotations.NotNull;

public record BooleanValue(boolean wrapped) implements Value {
    @Override
    public int compareTo(final @NotNull Value o) {
        if (o instanceof BooleanValue) {
            return Boolean.compare(this.wrapped, ((BooleanValue) o).wrapped);
        }

        return -1;
    }
}
