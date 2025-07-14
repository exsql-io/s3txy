package io.exsql.s3xty;

public enum OperationCode {
    // General Operations
    HALT,
    // Memory Operations
    LOAD,
    GET_FIELD,
    // Equality Operations
    BOOLEAN_EQ,
    LONG_EQ,
    DOUBLE_EQ,
    STRING_EQ,
}
