package io.exsql.s3xty;

import org.apache.spark.sql.types.DataTypes;

public final class SExpressionVM {

    public static final int DEFAULT_STACK_SIZE = 256;

    private final Value[] stack = new Value[DEFAULT_STACK_SIZE];

    private int sp = 0;

    public SExpressionVM() {}

    public void evaluate(final Program program, final CachedValueBag bag) {
        Value register1;
        Value register2;

        while (program.hasNext()) {
            var instruction = program.next();
            switch (instruction.operation()) {
                case LOAD:
                    push(instruction.operands()[0]);
                    break;
                case GET_FIELD:
                    register1 = pop(); // field position
                    register2 = pop(); // field type

                    var dataType = ((FieldTypeValue) register2).dataType();
                    if (dataType.equals(DataTypes.LongType)) {
                        var optional = bag.getLong(((StringValue) register1).wrapped());
                        if (optional.isPresent()) {
                            push(Value.longValue(optional.getAsLong()));
                        } else {
                            push(Value.nullValue());
                        }
                    } else if (dataType.equals(DataTypes.DoubleType)) {
                        var optional = bag.getDouble(((StringValue) register1).wrapped());
                        if (optional.isPresent()) {
                            push(Value.doubleValue(optional.getAsDouble()));
                        } else {
                            push(Value.nullValue());
                        }
                    } else if (dataType.equals(DataTypes.BooleanType)) {
                        push(Value.booleanValue(bag.getBoolean(((StringValue) register1).wrapped())));
                    } else {
                        var string = bag.get(((StringValue) register1).wrapped());
                        if (string != null) {
                            push(string);
                        } else {
                            push(Value.nullValue());
                        }
                    }
                    break;
                case LONG_EQ:
                    register1 = pop(); // left operand
                    register2 = pop(); // right operand
                    push(Value.booleanValue(((LongValue) register1).wrapped() == ((LongValue) register2).wrapped()));
                    break;
                case DOUBLE_EQ:
                    register1 = pop(); // left operand
                    register2 = pop(); // right operand
                    push(Value.booleanValue(((DoubleValue) register1).wrapped() == ((DoubleValue) register2).wrapped()));
                    break;
                case BOOLEAN_EQ:
                    register1 = pop(); // left operand
                    register2 = pop(); // right operand
                    push(Value.booleanValue(((BooleanValue) register1).wrapped() == ((BooleanValue) register2).wrapped()));
                    break;
                case STRING_EQ:
                    register1 = pop(); // left operand
                    register2 = pop(); // right operand
                    push(Value.booleanValue(((StringValue) register1).wrapped().equals(((StringValue) register2).wrapped())));
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
