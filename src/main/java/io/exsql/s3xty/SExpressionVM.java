package io.exsql.s3xty;

import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * A virtual machine for evaluating S-expressions.
 * This VM is stack-based and uses a simple instruction set.
 */
public final class SExpressionVM {

    private static final Logger LOGGER = LoggerFactory.getLogger(SExpressionVM.class);

    public static final int DEFAULT_STACK_SIZE = 256;

    // Stack for operands
    private final Value[] stack = new Value[DEFAULT_STACK_SIZE];
    private int sp = 0;

    // Execution context
    private CachedValueBag valueBag;
    
    // Instruction handlers
    private final Map<OperationCode, InstructionHandler> instructionHandlers = new HashMap<>();
    private final Program program;
    private final boolean useVectorAPI;

    // Results array for storing multiple expression results
    private final boolean[] results;

    /**
     * Creates a new SExpressionVM with the default instruction handlers.
     */
    public SExpressionVM(final Map<String, String> environment, final Program program) {
        this.program = program;
        this.results = program.output();
        this.useVectorAPI = Boolean.parseBoolean(environment.getOrDefault("S3XTY_VM_USE_VECTOR_API", "false"));
        registerDefaultInstructionHandlers();
    }

    /**
     * Evaluates a program with the given value bag.
     *
     * @param bag the value bag to use for field lookups
     */
    public void evaluate(final CachedValueBag bag) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Evaluating: \n{}", program);
        }

        this.valueBag = bag;

        while (program.hasNext()) {
            var instruction = program.next();
            var operation = instruction.operation();
            
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Executing instruction: {} at index {}", instruction, program.getCurrentIndex() - 1);
            }
            
            var handler = instructionHandlers.get(operation);
            if (handler != null) {
                handler.execute(this, program, instruction);
            } else {
                throw new IllegalStateException("Unknown operation: " + operation);
            }
        }
    }

    /**
     * Returns the result of the program execution.
     * The result is the boolean value at the top of the stack.
     *
     * @return the result of the program execution
     */
    public boolean result() {
        return this.stack[0] != null && ((BooleanValue) this.stack[0]).wrapped();
    }
    
    /**
     * Returns the results of evaluating multiple S-expressions.
     * The results are stored in the order they were evaluated.
     *
     * @return the array of results
     */
    public boolean[] results() {
        return this.results;
    }

    /**
     * Resets the VM state.
     */
    public void reset() {
        this.sp = 0;
        this.valueBag = null;
    }

    /**
     * Pushes a value onto the stack.
     *
     * @param value the value to push
     * @throws StackOverflowError if the stack is full
     */
    public void push(final Value value) {
        if (sp >= stack.length) {
            throw new StackOverflowError("VM stack overflow");
        }
        this.stack[this.sp++] = value;
    }

    /**
     * Pops a value from the stack.
     *
     * @return the value popped from the stack
     * @throws IllegalStateException if the stack is empty
     */
    public Value pop() {
        if (sp <= 0) {
            throw new IllegalStateException("VM stack underflow");
        }
        return this.stack[--this.sp];
    }
    
    /**
     * Returns the value bag used for field lookups.
     *
     * @return the value bag
     */
    public CachedValueBag getValueBag() {
        return this.valueBag;
    }
    
    /**
     * Duplicates the top value on the stack.
     */
    public void dup() {
        if (sp <= 0) {
            throw new IllegalStateException("Cannot duplicate: stack is empty");
        }

        push(this.stack[this.sp - 1]);
    }
    
    /**
     * Registers the default instruction handlers.
     */
    private void registerDefaultInstructionHandlers() {
        // General Operations
        instructionHandlers.put(OperationCode.HALT, (vm, program, instruction) -> {
            // Do nothing, just halt execution
        });
        
        // Memory Operations
        instructionHandlers.put(OperationCode.LOAD, (vm, program, instruction) -> vm.push(instruction.operand(0)));
        instructionHandlers.put(OperationCode.GET_FIELD, (vm, program, instruction) -> {
            var fieldPosition = vm.pop();
            var fieldType = vm.pop();
            var dataType = ((FieldTypeValue) fieldType).dataType();

            if (dataType.equals(DataTypes.LongType)) {
                var optional = vm.getValueBag().getLong(((StringValue) fieldPosition).wrapped());
                if (optional.isPresent()) {
                    vm.push(Value.longValue(optional.getAsLong()));
                } else {
                    vm.push(Value.nullValue());
                }
            } else if (dataType.equals(DataTypes.DoubleType)) {
                var optional = vm.getValueBag().getDouble(((StringValue) fieldPosition).wrapped());
                if (optional.isPresent()) {
                    vm.push(Value.doubleValue(optional.getAsDouble()));
                } else {
                    vm.push(Value.nullValue());
                }
            } else if (dataType.equals(DataTypes.BooleanType)) {
                vm.push(Value.booleanValue(vm.getValueBag().getBoolean(((StringValue) fieldPosition).wrapped())));
            } else {
                var string = vm.getValueBag().get(((StringValue) fieldPosition).wrapped());
                vm.push(Objects.requireNonNullElseGet(string, Value::nullValue));
            }
        });
        
        // Combining Operations
        instructionHandlers.put(OperationCode.NOT, (vm, program, instruction) -> vm.push(Value.booleanValue(!((BooleanValue) vm.pop()).wrapped())));
        
        // Control Flow Operations
        instructionHandlers.put(OperationCode.JUMP_IF_TRUE, (vm, program, instruction) -> {
            if (((BooleanValue) vm.pop()).wrapped()) {
                int jumpTarget = (int) ((LongValue) instruction.operand(0)).wrapped();
                program.setCurrentIndex(jumpTarget);
            }
        });
        
        instructionHandlers.put(OperationCode.JUMP_IF_FALSE, (vm, program, instruction) -> {
            if (!((BooleanValue) vm.pop()).wrapped()) {
                int jumpTarget = (int) ((LongValue) instruction.operand(0)).wrapped();
                program.setCurrentIndex(jumpTarget);
            }
        });

        // Stack Operations
        instructionHandlers.put(OperationCode.DUP, (vm, program, instruction) -> vm.dup());
        instructionHandlers.put(OperationCode.POP, (vm, program, instruction) -> vm.pop());
        
        // Result Operations
        instructionHandlers.put(OperationCode.STORE_RESULT, (vm, program, instruction) -> {
            int index = (int) ((LongValue) instruction.operand(0)).wrapped();
            boolean result = ((BooleanValue) vm.stack[vm.sp - 1]).wrapped();
            
            // Store the result
            vm.results[index] = result;
        });
        
        // Register binary operations
        registerBinaryOperation(OperationCode.LONG_EQ, Operation::nullSafeLongEq);
        registerBinaryOperation(OperationCode.DOUBLE_EQ, Operation::nullSafeDoubleEq);
        registerBinaryOperation(OperationCode.BOOLEAN_EQ, Operation::nullSafeBooleanEq);
        registerBinaryOperation(OperationCode.STRING_EQ, (v1, v2) -> Operation.nullSafeStringEq(v1, v2, useVectorAPI));
        registerBinaryOperation(OperationCode.LONG_NE, (v1, v2) -> !Operation.nullSafeLongEq(v1, v2));
        registerBinaryOperation(OperationCode.DOUBLE_NE, (v1, v2) -> !Operation.nullSafeDoubleEq(v1, v2));
        registerBinaryOperation(OperationCode.BOOLEAN_NE, (v1, v2) -> !Operation.nullSafeBooleanEq(v1, v2));
        registerBinaryOperation(OperationCode.STRING_NE, (v1, v2) -> !Operation.nullSafeStringEq(v1, v2, useVectorAPI));
        registerBinaryOperation(OperationCode.LONG_LT, Operation::nullSafeLongLt);
        registerBinaryOperation(OperationCode.DOUBLE_LT, Operation::nullSafeDoubleLt);
        registerBinaryOperation(OperationCode.STRING_LT, (v1, v2) -> Operation.nullSafeStringLt(v1, v2, useVectorAPI));
        registerBinaryOperation(OperationCode.LONG_LE, Operation::nullSafeLongLe);
        registerBinaryOperation(OperationCode.DOUBLE_LE, Operation::nullSafeDoubleLe);
        registerBinaryOperation(OperationCode.STRING_LE, (v1, v2) -> Operation.nullSafeStringLe(v1, v2, useVectorAPI));
        registerBinaryOperation(OperationCode.LONG_GT, Operation::nullSafeLongGt);
        registerBinaryOperation(OperationCode.DOUBLE_GT, Operation::nullSafeDoubleGt);
        registerBinaryOperation(OperationCode.STRING_GT, (v1, v2) -> Operation.nullSafeStringGt(v1, v2, useVectorAPI));
        registerBinaryOperation(OperationCode.LONG_GE, Operation::nullSafeLongGe);
        registerBinaryOperation(OperationCode.DOUBLE_GE, Operation::nullSafeDoubleGe);
        registerBinaryOperation(OperationCode.STRING_GE, (v1, v2) -> Operation.nullSafeStringGe(v1, v2, useVectorAPI));
    }
    
    /**
     * Registers a binary operation handler.
     *
     * @param opCode the operation code
     * @param operation the operation function that takes two values and returns a boolean result
     */
    private void registerBinaryOperation(OperationCode opCode, BiFunction<Value, Value, Boolean> operation) {
        instructionHandlers.put(opCode, (vm, program, instruction) -> vm.push(Value.booleanValue(operation.apply(vm.pop(), vm.pop()))));
    }
    
    /**
     * Interface for instruction handlers.
     */
    @FunctionalInterface
    private interface InstructionHandler {
        void execute(SExpressionVM vm, Program program, Instruction instruction);
    }

}
