package io.exsql.s3xty;

import org.jetbrains.annotations.NotNull;

public record NullValue() implements Value {
    @Override
    public @NotNull String toString() {
        return "null";
    }
}
