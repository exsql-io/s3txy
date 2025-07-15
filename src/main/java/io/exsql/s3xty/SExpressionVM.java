package io.exsql.s3xty;

import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public final class SExpressionVM {

    private final Logger LOGGER = LoggerFactory.getLogger(SExpressionVM.class);

    public static final int DEFAULT_STACK_SIZE = 256;

    private final Value[] stack = new Value[DEFAULT_STACK_SIZE];

    private int sp = 0;

    public SExpressionVM() {}

    public void evaluate(final Program program, final CachedValueBag bag) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Evaluating: \n{}", program);
        }

        Value register1;
        Value register2;

        while (program.hasNext()) {
            var instruction = program.next();
            var operation = instruction.operation();
            switch (operation) {
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
                        push(Objects.requireNonNullElseGet(string, Value::nullValue));
                    }
                    break;
                case NOT:
                    register1 = pop();
                    push(Value.booleanValue(!((BooleanValue) register1).wrapped()));
                    break;
                case OR:
                    register1 = pop();
                    register2 = pop();
                    push(Value.booleanValue(((BooleanValue) register1).wrapped() || ((BooleanValue) register2).wrapped()));
                    break;
                case AND:
                    register1 = pop();
                    register2 = pop();
                    push(Value.booleanValue(((BooleanValue) register1).wrapped() && ((BooleanValue) register2).wrapped()));
                    break;
                case HALT:
                    return;
                default:
                    // The most common case is binary operator
                    register1 = pop(); // left operand
                    register2 = pop(); // right operand
                    switch (operation) {
                        case LONG_EQ:
                            push(Value.booleanValue(((LongValue) register1).wrapped() == ((LongValue) register2).wrapped()));
                            break;
                        case DOUBLE_EQ:
                            push(Value.booleanValue(((DoubleValue) register1).wrapped() == ((DoubleValue) register2).wrapped()));
                            break;
                        case BOOLEAN_EQ:
                            push(Value.booleanValue(((BooleanValue) register1).wrapped() == ((BooleanValue) register2).wrapped()));
                            break;
                        case STRING_EQ:
                            push(Value.booleanValue(((StringValue) register1).wrapped().equals(((StringValue) register2).wrapped())));
                            break;
                        case LONG_NE:
                            push(Value.booleanValue(((LongValue) register1).wrapped() != ((LongValue) register2).wrapped()));
                            break;
                        case DOUBLE_NE:
                            push(Value.booleanValue(((DoubleValue) register1).wrapped() != ((DoubleValue) register2).wrapped()));
                            break;
                        case BOOLEAN_NE:
                            push(Value.booleanValue(((BooleanValue) register1).wrapped() != ((BooleanValue) register2).wrapped()));
                            break;
                        case STRING_NE:
                            push(Value.booleanValue(!((StringValue) register1).wrapped().equals(((StringValue) register2).wrapped())));
                            break;
                        case LONG_LT:
                            push(Value.booleanValue(((LongValue) register1).wrapped() < ((LongValue) register2).wrapped()));
                            break;
                        case DOUBLE_LT:
                            push(Value.booleanValue(((DoubleValue) register1).wrapped() < ((DoubleValue) register2).wrapped()));
                            break;
                        case STRING_LT:
                            push(Value.booleanValue(((StringValue) register1).wrapped().compareTo(((StringValue) register2).wrapped()) < 0));
                            break;
                        case LONG_LE:
                            push(Value.booleanValue(((LongValue) register1).wrapped() <= ((LongValue) register2).wrapped()));
                            break;
                        case DOUBLE_LE:
                            push(Value.booleanValue(((DoubleValue) register1).wrapped() <= ((DoubleValue) register2).wrapped()));
                            break;
                        case STRING_LE:
                            push(Value.booleanValue(((StringValue) register1).wrapped().compareTo(((StringValue) register2).wrapped()) <= 0));
                            break;
                        case LONG_GT:
                            push(Value.booleanValue(((LongValue) register1).wrapped() > ((LongValue) register2).wrapped()));
                            break;
                        case DOUBLE_GT:
                            push(Value.booleanValue(((DoubleValue) register1).wrapped() > ((DoubleValue) register2).wrapped()));
                            break;
                        case STRING_GT:
                            push(Value.booleanValue(((StringValue) register1).wrapped().compareTo(((StringValue) register2).wrapped()) > 0));
                            break;
                        case LONG_GE:
                            push(Value.booleanValue(((LongValue) register1).wrapped() >= ((LongValue) register2).wrapped()));
                            break;
                        case DOUBLE_GE:
                            push(Value.booleanValue(((DoubleValue) register1).wrapped() >= ((DoubleValue) register2).wrapped()));
                            break;
                        case STRING_GE:
                            push(Value.booleanValue(((StringValue) register1).wrapped().compareTo(((StringValue) register2).wrapped()) >= 0));
                            break;
                    }
                    break;
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
