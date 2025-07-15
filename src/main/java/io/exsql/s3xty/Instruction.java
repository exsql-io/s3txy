package io.exsql.s3xty;

import org.jetbrains.annotations.NotNull;

public record Instruction(OperationCode operation, Value[] operands) {

    public static Instruction halt() {
        return new Instruction(OperationCode.HALT, null);
    }

    public static Instruction load(final Value value) {
        return new Instruction(OperationCode.LOAD, new Value[]{value});
    }

    public static Instruction getField() {
        return new Instruction(OperationCode.GET_FIELD, null);
    }

    public static Instruction longEqual() {
        return new Instruction(OperationCode.LONG_EQ, null);
    }

    public static Instruction stringEqual() {
        return new Instruction(OperationCode.STRING_EQ, null);
    }

    public static Instruction doubleEqual() {
        return new Instruction(OperationCode.DOUBLE_EQ, null);
    }

    public static Instruction booleanEqual() {
        return new Instruction(OperationCode.BOOLEAN_EQ, null);
    }

    public static Instruction longNotEqual() {
        return new Instruction(OperationCode.LONG_NE, null);
    }

    public static Instruction stringNotEqual() {
        return new Instruction(OperationCode.STRING_NE, null);
    }

    public static Instruction doubleNotEqual() {
        return new Instruction(OperationCode.DOUBLE_NE, null);
    }

    public static Instruction booleanNotEqual() {
        return new Instruction(OperationCode.BOOLEAN_NE, null);
    }

    public static Instruction longLesserThan() {
        return new Instruction(OperationCode.LONG_LT, null);
    }

    public static Instruction stringLesserThan() {
        return new Instruction(OperationCode.STRING_LT, null);
    }

    public static Instruction doubleLesserThan() {
        return new Instruction(OperationCode.DOUBLE_LT, null);
    }

    public static Instruction longLesserThanOrEqual() {
        return new Instruction(OperationCode.LONG_LE, null);
    }

    public static Instruction stringLesserThanOrEqual() {
        return new Instruction(OperationCode.STRING_LE, null);
    }

    public static Instruction doubleLesserThanOrEqual() {
        return new Instruction(OperationCode.DOUBLE_LE, null);
    }

    public static Instruction longGreaterThan() {
        return new Instruction(OperationCode.LONG_GT, null);
    }

    public static Instruction stringGreaterThan() {
        return new Instruction(OperationCode.STRING_GT, null);
    }

    public static Instruction doubleGreaterThan() {
        return new Instruction(OperationCode.DOUBLE_GT, null);
    }

    public static Instruction longGreaterThanOrEqual() {
        return new Instruction(OperationCode.LONG_GE, null);
    }

    public static Instruction stringGreaterThanOrEqual() {
        return new Instruction(OperationCode.STRING_GE, null);
    }

    public static Instruction doubleGreaterThanOrEqual() {
        return new Instruction(OperationCode.DOUBLE_GE, null);
    }

    public static Instruction not() {
        return new Instruction(OperationCode.NOT, null);
    }

    public static Instruction or() {
        return new Instruction(OperationCode.OR, null);
    }

    public static Instruction and() {
        return new Instruction(OperationCode.AND, null);
    }

    @Override
    public @NotNull String toString() {
        var operandsToString = new StringBuilder();
        if (this.operands != null) {
            for (int i = 0; i < this.operands.length; i++) {
                operandsToString.append(this.operands[i]);
                if (i > 0) {
                    operandsToString.append(", ");
                }
            }
        }

        return String.format("%s(%s)", this.operation, operandsToString);
    }

}
