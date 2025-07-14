package io.exsql.s3xty;

import org.jetbrains.annotations.NotNull;

public record IntValue(int wrapped) implements Value {

    @Override
    public int compareTo(@NotNull final Value o) {
        if (o instanceof IntValue) {
            return Integer.compare(this.wrapped, ((IntValue) o).wrapped);
        }

        return -1;
    }

    @Override
    public String toString() {
        return String.format("int(%d)", this.wrapped);
    }

}
