package io.exsql.s3xty;

public record Instruction(OperationCode operation, Value[] operands) {

    public static Instruction halt() {
        return new Instruction(OperationCode.HALT, null);
    }

    public  static Instruction load(final Value value) {
        return new Instruction(OperationCode.LOAD, new Value[]{value});
    }

    public  static Instruction equal() {
        return new Instruction(OperationCode.EQUAL, null);
    }

    public static Instruction getField() {
        return new Instruction(OperationCode.GET_FIELD, null);
    }

    public  static Instruction longEqual() {
        return new Instruction(OperationCode.LONG_EQ, null);
    }

}
