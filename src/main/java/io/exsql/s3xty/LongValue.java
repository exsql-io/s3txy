package io.exsql.s3xty;

import org.jetbrains.annotations.NotNull;

public record LongValue(long wrapped) implements Value {
    @Override
    public int compareTo(@NotNull Value o) {
        if (o instanceof LongValue) {
            return Long.compare(this.wrapped, ((LongValue) o).wrapped);
        }

        return -1;
    }
}
