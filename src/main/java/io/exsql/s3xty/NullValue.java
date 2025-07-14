package io.exsql.s3xty;

public record NullValue() implements Value {
    @Override
    public String toString() {
        return "null";
    }
}
