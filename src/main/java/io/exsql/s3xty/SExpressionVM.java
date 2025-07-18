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

    /**
     * Creates a new SExpressionVM with the default instruction handlers.
     */
    public SExpressionVM() {
        registerDefaultInstructionHandlers();
    }

    /**
     * Evaluates a program with the given value bag.
     *
     * @param program the program to evaluate
     * @param bag the value bag to use for field lookups
     */
    public void evaluate(final Program program, final CachedValueBag bag) {
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
            
            InstructionHandler handler = instructionHandlers.get(operation);
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
        Value topValue = this.stack[this.sp - 1];
        push(topValue);
    }
    
    /**
     * Registers the default instruction handlers.
     */
    private void registerDefaultInstructionHandlers() {
        // General Operations
        instructionHandlers.put(OperationCode.HALT, (_, _, _) -> {
            // Do nothing, just halt execution
        });
        
        // Memory Operations
        instructionHandlers.put(OperationCode.LOAD, (vm, _, instruction) -> vm.push(instruction.operand(0)));
        
        instructionHandlers.put(OperationCode.GET_FIELD, (vm, _, _) -> {
            Value fieldPosition = vm.pop();
            Value fieldType = vm.pop();
            
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
        instructionHandlers.put(OperationCode.NOT, (vm, _, _) -> {
            Value value = vm.pop();
            vm.push(Value.booleanValue(!((BooleanValue) value).wrapped()));
        });
        
        // Control Flow Operations
        
        instructionHandlers.put(OperationCode.JUMP_IF_TRUE, (vm, program, instruction) -> {
            Value condition = vm.pop();
            if (((BooleanValue) condition).wrapped()) {
                int jumpTarget = (int) ((LongValue) instruction.operand(0)).wrapped();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Condition is true, jumping to instruction index: {}", jumpTarget);
                }
                program.setCurrentIndex(jumpTarget);
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Condition is false, not jumping");
            }
        });
        
        instructionHandlers.put(OperationCode.JUMP_IF_FALSE, (vm, program, instruction) -> {
            Value condition = vm.pop();
            if (!((BooleanValue) condition).wrapped()) {
                int jumpTarget = (int) ((LongValue) instruction.operand(0)).wrapped();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Condition is false, jumping to instruction index: {}", jumpTarget);
                }
                program.setCurrentIndex(jumpTarget);
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Condition is true, not jumping");
            }
        });
        
        
        // Stack Operations
        instructionHandlers.put(OperationCode.DUP, (vm, _, _) -> vm.dup());
        instructionHandlers.put(OperationCode.POP, (vm, _, _) -> vm.pop());
        
        // Register binary operations
        registerBinaryOperation(OperationCode.LONG_EQ, 
            (v1, v2) -> ((LongValue)v1).wrapped() == ((LongValue)v2).wrapped());
        registerBinaryOperation(OperationCode.DOUBLE_EQ, 
            (v1, v2) -> ((DoubleValue)v1).wrapped() == ((DoubleValue)v2).wrapped());
        registerBinaryOperation(OperationCode.BOOLEAN_EQ, 
            (v1, v2) -> ((BooleanValue)v1).wrapped() == ((BooleanValue)v2).wrapped());
        registerBinaryOperation(OperationCode.STRING_EQ, 
            (v1, v2) -> ((StringValue)v1).wrapped().equals(((StringValue)v2).wrapped()));
            
        registerBinaryOperation(OperationCode.LONG_NE, 
            (v1, v2) -> ((LongValue)v1).wrapped() != ((LongValue)v2).wrapped());
        registerBinaryOperation(OperationCode.DOUBLE_NE, 
            (v1, v2) -> ((DoubleValue)v1).wrapped() != ((DoubleValue)v2).wrapped());
        registerBinaryOperation(OperationCode.BOOLEAN_NE, 
            (v1, v2) -> ((BooleanValue)v1).wrapped() != ((BooleanValue)v2).wrapped());
        registerBinaryOperation(OperationCode.STRING_NE, 
            (v1, v2) -> !((StringValue)v1).wrapped().equals(((StringValue)v2).wrapped()));
            
        registerBinaryOperation(OperationCode.LONG_LT, 
            (v1, v2) -> ((LongValue)v1).wrapped() < ((LongValue)v2).wrapped());
        registerBinaryOperation(OperationCode.DOUBLE_LT, 
            (v1, v2) -> ((DoubleValue)v1).wrapped() < ((DoubleValue)v2).wrapped());
        registerBinaryOperation(OperationCode.STRING_LT, 
            (v1, v2) -> ((StringValue)v1).wrapped().compareTo(((StringValue)v2).wrapped()) < 0);
            
        registerBinaryOperation(OperationCode.LONG_LE, 
            (v1, v2) -> ((LongValue)v1).wrapped() <= ((LongValue)v2).wrapped());
        registerBinaryOperation(OperationCode.DOUBLE_LE, 
            (v1, v2) -> ((DoubleValue)v1).wrapped() <= ((DoubleValue)v2).wrapped());
        registerBinaryOperation(OperationCode.STRING_LE, 
            (v1, v2) -> ((StringValue)v1).wrapped().compareTo(((StringValue)v2).wrapped()) <= 0);
            
        registerBinaryOperation(OperationCode.LONG_GT, 
            (v1, v2) -> ((LongValue)v1).wrapped() > ((LongValue)v2).wrapped());
        registerBinaryOperation(OperationCode.DOUBLE_GT, 
            (v1, v2) -> ((DoubleValue)v1).wrapped() > ((DoubleValue)v2).wrapped());
        registerBinaryOperation(OperationCode.STRING_GT, 
            (v1, v2) -> ((StringValue)v1).wrapped().compareTo(((StringValue)v2).wrapped()) > 0);
            
        registerBinaryOperation(OperationCode.LONG_GE, 
            (v1, v2) -> ((LongValue)v1).wrapped() >= ((LongValue)v2).wrapped());
        registerBinaryOperation(OperationCode.DOUBLE_GE, 
            (v1, v2) -> ((DoubleValue)v1).wrapped() >= ((DoubleValue)v2).wrapped());
        registerBinaryOperation(OperationCode.STRING_GE, 
            (v1, v2) -> ((StringValue)v1).wrapped().compareTo(((StringValue)v2).wrapped()) >= 0);
    }
    
    /**
     * Registers a binary operation handler.
     *
     * @param opCode the operation code
     * @param operation the operation function that takes two values and returns a boolean result
     */
    private void registerBinaryOperation(OperationCode opCode, BiFunction<Value, Value, Boolean> operation) {
        instructionHandlers.put(opCode, (vm, _, _) -> {
            Value v1 = vm.pop();
            Value v2 = vm.pop();
            vm.push(Value.booleanValue(operation.apply(v1, v2)));
        });
    }
    
    /**
     * Interface for instruction handlers.
     */
    @FunctionalInterface
    private interface InstructionHandler {
        void execute(SExpressionVM vm, Program program, Instruction instruction);
    }

}
