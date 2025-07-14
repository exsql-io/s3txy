package io.exsql.s3xty;

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

    public static Instruction not() {
        return new Instruction(OperationCode.NOT, null);
    }
    
    @Override
    public String toString() {
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
