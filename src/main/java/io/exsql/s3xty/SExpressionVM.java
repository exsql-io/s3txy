package io.exsql.s3xty;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

public final class SExpressionVM {

    public static final int DEFAULT_STACK_SIZE = 256;

    private final Value[] stack = new Value[DEFAULT_STACK_SIZE];

    private int sp = 0;

    public SExpressionVM() {}

    public void evaluate(final Program program, final Row row) {
        Value register1;
        Value register2;

        while (program.hasNext()) {
            var instruction = program.next();
            switch (instruction.operation()) {
                case LOAD:
                    push(instruction.operands()[0]);
                    break;
                case EQUAL:
                    register1 = pop(); // left operand
                    register2 = pop(); // right operand
                    push(Value.booleanValue(register1.equals(register2)));
                    break;
                case GET_FIELD:
                    register1 = pop(); // field position
                    register2 = pop(); // field type
                    if (((FieldTypeValue) register2).dataType().equals(DataTypes.LongType)) {
                        push(Value.longValue(row.getLong(((IntValue) register1).wrapped())));
                    }
                    break;
                case LONG_EQ:
                    register1 = pop(); // left operand
                    register2 = pop(); // right operand
                    push(Value.booleanValue(((LongValue) register1).wrapped() == ((LongValue) register2).wrapped()));
                    break;
                case HALT:
                    return;
            }
        }
    }

    public boolean result() {
        return this.stack[0] != null && ((BooleanValue) this.stack[0]).wrapped();
    }

    public void reset() {
        this.sp = 0;
    }

    private void push(final Value value) {
        this.stack[this.sp++] = value;
    }

    private Value pop() {
        return this.stack[--this.sp];
    }

}
