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
    // Inequality Operations
    BOOLEAN_NE,
    LONG_NE,
    DOUBLE_NE,
    STRING_NE,
    // Comparison Operations
    LONG_LT,
    DOUBLE_LT,
    STRING_LT,
    LONG_LE,
    DOUBLE_LE,
    STRING_LE,
    LONG_GT,
    DOUBLE_GT,
    STRING_GT,
    LONG_GE,
    DOUBLE_GE,
    STRING_GE,
    // Combining Operations
    NOT,
    OR,
    AND
}
