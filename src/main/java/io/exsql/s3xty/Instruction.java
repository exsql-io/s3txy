package io.exsql.s3xty;

import io.exsql.s3xty.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

/**
 * Represents an instruction in the VM.
 * An instruction consists of an operation code and optional operands.
 */
public record Instruction(OperationCode operation, @Nullable Value[] operands) implements Serializable {

    /**
     * Creates a new instruction with the given operation code and no operands.
     *
     * @param operation the operation code
     * @return a new instruction
     */
    public static Instruction create(final OperationCode operation) {
        return new Instruction(operation, null);
    }

    /**
     * Creates a new instruction with the given operation code and operands.
     *
     * @param operation the operation code
     * @param operands the operands
     * @return a new instruction
     */
    public static Instruction create(final OperationCode operation, Value... operands) {
        return new Instruction(operation, operands);
    }


    /**
     * Returns the operand at the given index.
     *
     * @param index the index of the operand
     * @return the operand at the given index
     * @throws IndexOutOfBoundsException if the index is out of bounds
     */
    public Value operand(int index) {
        if (operands == null || index < 0 || index >= operands.length) {
            throw new IndexOutOfBoundsException("Invalid operand index: " + index);
        }
        return operands[index];
    }

    // Factory methods for common instructions

    public static Instruction halt() {
        return create(OperationCode.HALT);
    }

    public static Instruction load(final Value value) {
        return create(OperationCode.LOAD, value);
    }

    public static Instruction getField() {
        return create(OperationCode.GET_FIELD);
    }

    public static Instruction longEqual() {
        return create(OperationCode.LONG_EQ);
    }

    public static Instruction stringEqual() {
        return create(OperationCode.STRING_EQ);
    }

    public static Instruction doubleEqual() {
        return create(OperationCode.DOUBLE_EQ);
    }

    public static Instruction booleanEqual() {
        return create(OperationCode.BOOLEAN_EQ);
    }

    public static Instruction longNotEqual() {
        return create(OperationCode.LONG_NE);
    }

    public static Instruction stringNotEqual() {
        return create(OperationCode.STRING_NE);
    }

    public static Instruction doubleNotEqual() {
        return create(OperationCode.DOUBLE_NE);
    }

    public static Instruction booleanNotEqual() {
        return create(OperationCode.BOOLEAN_NE);
    }

    public static Instruction longLesserThan() {
        return create(OperationCode.LONG_LT);
    }

    public static Instruction stringLesserThan() {
        return create(OperationCode.STRING_LT);
    }

    public static Instruction doubleLesserThan() {
        return create(OperationCode.DOUBLE_LT);
    }

    public static Instruction longLesserThanOrEqual() {
        return create(OperationCode.LONG_LE);
    }

    public static Instruction stringLesserThanOrEqual() {
        return create(OperationCode.STRING_LE);
    }

    public static Instruction doubleLesserThanOrEqual() {
        return create(OperationCode.DOUBLE_LE);
    }

    public static Instruction longGreaterThan() {
        return create(OperationCode.LONG_GT);
    }

    public static Instruction stringGreaterThan() {
        return create(OperationCode.STRING_GT);
    }

    public static Instruction doubleGreaterThan() {
        return create(OperationCode.DOUBLE_GT);
    }

    public static Instruction longGreaterThanOrEqual() {
        return create(OperationCode.LONG_GE);
    }

    public static Instruction stringGreaterThanOrEqual() {
        return create(OperationCode.STRING_GE);
    }

    public static Instruction doubleGreaterThanOrEqual() {
        return create(OperationCode.DOUBLE_GE);
    }

    public static Instruction isNotNull() {
        return create(OperationCode.IS_NOT_NULL);
    }

    public static Instruction stringCiEqual() {
        return create(OperationCode.STRING_CI_EQ);
    }

    public static Instruction stringRegexpMatch() {
        return create(OperationCode.STRING_REGEXP_MATCH);
    }

    public static Instruction not() {
        return create(OperationCode.NOT);
    }
    
    // Control flow instructions
    
    
    public static Instruction jumpIfTrue(final int targetIndex) {
        return create(OperationCode.JUMP_IF_TRUE, Value.longValue(targetIndex));
    }
    
    public static Instruction jumpIfFalse(final int targetIndex) {
        return create(OperationCode.JUMP_IF_FALSE, Value.longValue(targetIndex));
    }
    
    // Stack manipulation instructions
    
    public static Instruction dup() {
        return create(OperationCode.DUP);
    }
    
    public static Instruction pop() {
        return create(OperationCode.POP);
    }
    
    /**
     * Creates a new instruction for storing the top value on the stack in the results array.
     *
     * @param index the index in the results array where the value should be stored
     * @return a new instruction
     */
    public static Instruction storeResult(final int index) {
        return create(OperationCode.STORE_RESULT, Value.longValue(index));
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

        return String.format("%s(%s)", this.operation.name(), operandsToString);
    }

}
