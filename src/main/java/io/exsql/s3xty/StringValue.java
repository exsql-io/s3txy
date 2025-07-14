package io.exsql.s3xty;

import org.jetbrains.annotations.NotNull;

public record StringValue(String wrapped) implements Value {
    @Override
    public int compareTo(final @NotNull Value o) {
        if (o instanceof StringValue) {
            return CharSequence.compare(this.wrapped, ((StringValue) o).wrapped);
        }

        return -1;
    }
}
