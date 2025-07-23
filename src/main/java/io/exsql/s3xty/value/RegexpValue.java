package io.exsql.s3xty.value;

import com.google.re2j.Pattern;
import org.jetbrains.annotations.NotNull;

public record RegexpValue(Pattern pattern) implements Value {
    public boolean matches(final Value value) {
        if (value instanceof StringValue) return this.pattern.matches(((StringValue) value).wrapped().getBytes());
        return false;
    }

    @Override
    public @NotNull String toString() {
        return String.format("regexp(%s)", this.pattern.pattern());
    }
}
