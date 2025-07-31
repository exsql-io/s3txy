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
    STRING_CI_EQ,
    STRING_REGEXP_MATCH,
    // Contains Operations
    STRING_CONTAINS,
    STRING_CI_CONTAINS,
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
    // Control Flow Operations
    JUMP_IF_TRUE,   // Jump if top of stack is true
    JUMP_IF_FALSE,  // Jump if top of stack is false
    // Stack Operations
    DUP,            // Duplicate the top value on the stack
    POP,            // Remove the top value from the stack
    // Result Operations
    STORE_RESULT,    // Store the top value in the results array at the specified index
    // Nullability
    IS_NOT_NULL,
    // Collection operations
    STRING_ARRAY_CONTAINS,
    STRING_ARRAY_ELEMENT_CONTAINS,
    LONG_ARRAY_CONTAINS,
    DOUBLE_ARRAY_CONTAINS,
    BOOLEAN_ARRAY_CONTAINS,
}
