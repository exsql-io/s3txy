package io.exsql.s3xty;

import com.google.common.base.Stopwatch;
import io.exsql.s3xty.value.Value;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Compiler for S-expressions.
 * This compiler translates S-expressions into VM instructions.
 */
public final class Compiler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Compiler.class);

    private Compiler() {}

    /**
     * Compiles an S-expression into a program.
     *
     * @param schema the schema of the data
     * @param expression the S-expression to compile
     * @return the compiled program
     */
    public static Program compile(final StructType schema, final String expression) {
        return compile(schema, new String[] { expression });
    }
    
    /**
     * Compiles an array of S-expressions into a single program.
     * Each expression is compiled and its result is stored in the result array at the corresponding index.
     *
     * @param schema the schema of the data
     * @param expressions the array of S-expressions to compile
     * @return the compiled program with all expressions inlined
     */
    public static Program compile(final StructType schema, final String[] expressions) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null");
        }

        if (expressions == null || expressions.length == 0) {
            throw new IllegalArgumentException("Expressions array cannot be null or empty");
        }
        
        var stopWatch = Stopwatch.createStarted();
        var instructions = new ArrayList<Instruction>();
        try {
            for (var i = 0; i < expressions.length; i++) {
                String expression = expressions[i];
                if (expression == null || expression.isEmpty()) {
                    throw new IllegalArgumentException("Expression at index " + i + " cannot be null or empty");
                }
                
                // Parse and compile the expression
                var tokens = new StreamTokenizer(new StringReader(expression));
                while (tokens.nextToken() != StreamTokenizer.TT_EOF) {
                    parseExpression(tokens, instructions, schema);
                }
                
                // Store the result at the corresponding index in the result array
                instructions.add(Instruction.storeResult(i));
            }

            // Add the final halt instruction
            instructions.add(Instruction.halt());

            var elapsed = stopWatch.elapsed(TimeUnit.MILLISECONDS);
            LOGGER.debug("compile phase took: {}ms", elapsed);

            return new Program(expressions, instructions.toArray(new Instruction[0]));
        } catch (final Exception exception) {
            LOGGER.error("Error compiling expressions: {}", String.join("; ", expressions), exception);
            throw new RuntimeException("Error compiling expressions: " + exception, exception);
        }
    }

    private static void parseExpression(final StreamTokenizer tokens, final List<Instruction> instructions, final StructType schema) throws IOException {
        tokens.nextToken(); // skip (
        switch (tokens.sval) {
            case Keywords.NOT:
                tokens.nextToken(); // skip "not"
                parseExpression(tokens, instructions, schema);
                instructions.add(Instruction.not());
                break;
            case Keywords.OR:
                tokens.nextToken(); // skip "or"
                
                // Parse the first operand
                parseExpression(tokens, instructions, schema);
                
                // Keep track of all the jump instructions that need to be updated
                List<Integer> jumpIndices = new ArrayList<>();
                
                // Process all remaining operands
                while (tokens.ttype != ')') {
                    // Duplicate the top value for the conditional jump
                    instructions.add(Instruction.dup());
                    
                    // Add a conditional jump that will skip to the end if the current result is true
                    int jumpIfTrueIndex = instructions.size();
                    instructions.add(Instruction.jumpIfTrue(0)); // Placeholder will be updated later
                    jumpIndices.add(jumpIfTrueIndex);
                    
                    // Pop the duplicated value since we're evaluating the next operand
                    instructions.add(Instruction.pop());
                    
                    // Parse the next operand
                    parseExpression(tokens, instructions, schema);
                }
                
                // Update all jump targets to point to the instruction after all operands
                for (final int jumpIndex: jumpIndices) {
                    instructions.set(jumpIndex, Instruction.jumpIfTrue(instructions.size()));
                }
                break;
            case Keywords.AND:
                tokens.nextToken(); // skip "and"
                
                // Parse the first operand
                parseExpression(tokens, instructions, schema);
                
                // Keep track of all the jump instructions that need to be updated
                List<Integer> andJumpIndices = new ArrayList<>();
                
                // Process all remaining operands
                while (tokens.ttype != ')') {
                    // Duplicate the top value for the conditional jump
                    instructions.add(Instruction.dup());
                    
                    // Add a conditional jump that will skip to the end if the current result is false
                    int jumpIfFalseIndex = instructions.size();
                    instructions.add(Instruction.jumpIfFalse(0)); // Placeholder will be updated later
                    andJumpIndices.add(jumpIfFalseIndex);
                    
                    // Pop the duplicated value since we're evaluating the next operand
                    instructions.add(Instruction.pop());
                    
                    // Parse the next operand
                    parseExpression(tokens, instructions, schema);
                }
                
                // Update all jump targets to point to the instruction after all operands
                for (final int jumpIndex: andJumpIndices) {
                    instructions.set(jumpIndex, Instruction.jumpIfFalse(instructions.size()));
                }
                break;
            case Keywords.TRAIT_EXISTS:
                parseUnaryOperator(tokens, instructions, schema);
                break;
            default:
                parseBinaryOperator(tokens, instructions, schema);
                break;
        }
        tokens.nextToken(); // skip )
    }

    private static void parseUnaryOperator(final StreamTokenizer tokens, final List<Instruction> instructions, final StructType schema) throws IOException {
        var operator = tokens.sval;

        tokens.nextToken(); // consume operator
        parseGetField(tokens, instructions, schema); // parse the get field operation

        if (operator.equals(Keywords.TRAIT_EXISTS)) {
            instructions.add(Instruction.isNotNull());
            return;
        }

        throw new IllegalArgumentException("Unknown operator: " + operator);
    }

    private static void parseBinaryOperator(final StreamTokenizer tokens, final List<Instruction> instructions, final StructType schema) throws IOException {
        var operator = tokens.sval;

        tokens.nextToken(); // consume operator
        var dataType = parseGetField(tokens, instructions, schema); // parse the get field operation
        parseArgument(tokens, instructions, dataType, operator); // parse the constant value to check against

        switch (operator) {
            case Keywords.TRAIT_EQ:
                if (dataType.equals(DataTypes.LongType)) {
                    instructions.add(Instruction.longEqual());
                } else if (dataType.equals(DataTypes.DoubleType)) {
                    instructions.add(Instruction.doubleEqual());
                } else if (dataType.equals(DataTypes.BooleanType)) {
                    instructions.add(Instruction.booleanEqual());
                } else {
                    instructions.add(Instruction.stringEqual());
                }
                break;
            case Keywords.TRAIT_NE:
                if (dataType.equals(DataTypes.LongType)) {
                    instructions.add(Instruction.longNotEqual());
                } else if (dataType.equals(DataTypes.DoubleType)) {
                    instructions.add(Instruction.doubleNotEqual());
                } else if (dataType.equals(DataTypes.BooleanType)) {
                    instructions.add(Instruction.booleanNotEqual());
                } else {
                    instructions.add(Instruction.stringNotEqual());
                }
                break;
            case Keywords.TRAIT_LT:
                if (dataType.equals(DataTypes.LongType)) {
                    instructions.add(Instruction.longLesserThan());
                } else if (dataType.equals(DataTypes.DoubleType)) {
                    instructions.add(Instruction.doubleLesserThan());
                } else {
                    instructions.add(Instruction.stringLesserThan());
                }
                break;
            case Keywords.TRAIT_LE:
                if (dataType.equals(DataTypes.LongType)) {
                    instructions.add(Instruction.longLesserThanOrEqual());
                } else if (dataType.equals(DataTypes.DoubleType)) {
                    instructions.add(Instruction.doubleLesserThanOrEqual());
                } else {
                    instructions.add(Instruction.stringLesserThanOrEqual());
                }
                break;
            case Keywords.TRAIT_GT:
                if (dataType.equals(DataTypes.LongType)) {
                    instructions.add(Instruction.longGreaterThan());
                } else if (dataType.equals(DataTypes.DoubleType)) {
                    instructions.add(Instruction.doubleGreaterThan());
                } else {
                    instructions.add(Instruction.stringGreaterThan());
                }
                break;
            case Keywords.TRAIT_GE:
                if (dataType.equals(DataTypes.LongType)) {
                    instructions.add(Instruction.longGreaterThanOrEqual());
                } else if (dataType.equals(DataTypes.DoubleType)) {
                    instructions.add(Instruction.doubleGreaterThanOrEqual());
                } else {
                    instructions.add(Instruction.stringGreaterThanOrEqual());
                }
                break;
            case Keywords.TRAIT_CI_EQ:
                instructions.add(Instruction.stringCiEqual());
                break;
            case Keywords.TRAIT_REGEX:
                instructions.add(Instruction.stringRegexpMatch());
                break;
            case Keywords.TRAIT_CONTAINS:
                if (CachedArrayDataAccessor.LONG_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.longArrayContains());
                } else if (CachedArrayDataAccessor.DOUBLE_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.doubleArrayContains());
                } else if (CachedArrayDataAccessor.BOOLEAN_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.booleanArrayContains());
                } else if (CachedArrayDataAccessor.STRING_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.stringArrayContains());
                } else {
                    instructions.add(Instruction.stringContains());
                }
                break;
            case Keywords.TRAIT_CI_CONTAINS:
                instructions.add(Instruction.stringCiContains());
                break;
            case Keywords.TRAIT_ELEMENT_CONTAINS:
                instructions.add(Instruction.stringArrayElementContains());
                break;
            case Keywords.TRAIT_IN:
                if (DataTypes.LongType.equals(dataType)) {
                    instructions.add(Instruction.longIn());
                } else if (DataTypes.DoubleType.equals(dataType)) {
                    instructions.add(Instruction.doubleIn());
                } else if (DataTypes.BooleanType.equals(dataType)) {
                    instructions.add(Instruction.booleanIn());
                } else {
                    instructions.add(Instruction.stringIn());
                }
                break;
            case Keywords.TRAIT_CONTAINS_ANY:
                if (CachedArrayDataAccessor.LONG_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.longArrayIntersectsNonEmpty());
                } else if (CachedArrayDataAccessor.DOUBLE_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.doubleArrayIntersectsNonEmpty());
                } else if (CachedArrayDataAccessor.BOOLEAN_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.booleanArrayIntersectsNonEmpty());
                } else {
                    instructions.add(Instruction.stringArrayIntersectsNonEmpty());
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown operator: " + operator);
        }
    }

    private static DataType parseGetField(final StreamTokenizer tokens, final List<Instruction> instructions, final StructType schema) throws IOException {
        DataType dataType;
        try {
            var index = schema.fieldIndex(tokens.sval);
            dataType = schema.apply(index).dataType();
            instructions.add(Instruction.load(Value.fieldTypeValue(dataType)));
        } catch (final IllegalArgumentException illegalArgumentException) {
            // This is an acceptable path, schema is a hint and not mandatory.
            // String is the default type when not found in the schema.
            dataType = DataTypes.StringType;
            instructions.add(Instruction.load(Value.fieldTypeValue(dataType)));
        }

        instructions.add(Instruction.load(Value.stringValue(UTF8String.fromString(tokens.sval))));
        instructions.add(Instruction.getField());

        tokens.nextToken(); // consume get field operation

        return dataType;
    }

    private static void parseArgument(final StreamTokenizer tokens,
                                      final List<Instruction> instructions,
                                      final DataType dataType,
                                      final String operator) throws IOException {

        try {
            if (dataType != null) {
                if (operator.equals(Keywords.TRAIT_IN) || operator.equals(Keywords.TRAIT_CONTAINS_ANY)) {
                    parseMultiValueArgument(tokens, instructions, dataType);
                } else if (dataType.equals(DataTypes.LongType) || CachedArrayDataAccessor.LONG_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.load(Value.longValue(tokens.sval)));
                } else if (dataType.equals(DataTypes.DoubleType) || CachedArrayDataAccessor.DOUBLE_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.load(Value.doubleValue(tokens.sval)));
                } else if (dataType.equals(DataTypes.BooleanType) || CachedArrayDataAccessor.BOOLEAN_ARRAY_TYPE.sameType(dataType)) {
                    instructions.add(Instruction.load(Value.booleanValue(tokens.sval)));
                } else if (dataType.equals(DataTypes.StringType) || CachedArrayDataAccessor.STRING_ARRAY_TYPE.sameType(dataType)) {
                    parseStringArgument(tokens, instructions, operator);
                } else {
                    throw new IllegalArgumentException("Unsupported data type: " + dataType);
                }
            } else {
                parseStringArgument(tokens, instructions, operator);
            }
        } catch (final NumberFormatException nfe) {
            throw new IllegalArgumentException("Invalid value for type " + dataType + ": " + tokens.sval, nfe);
        }

        tokens.nextToken(); // consume argument
    }

    private static void parseStringArgument(final StreamTokenizer tokens, final List<Instruction> instructions, final String operator) {
        switch (operator) {
            case Keywords.TRAIT_CI_EQ -> instructions.add(Instruction.load(Value.stringValue(UTF8String.fromString(tokens.sval)).toLowercase()));
            case Keywords.TRAIT_REGEX -> instructions.add(Instruction.load(Value.regexpValue(tokens.sval)));
            default -> instructions.add(Instruction.load(Value.stringValue(UTF8String.fromString(tokens.sval))));
        }
    }

    private static void parseMultiValueArgument(final StreamTokenizer tokens, final List<Instruction> instructions, final DataType dataType) throws IOException {
        tokens.nextToken(); // consume (

        var elements = new ArrayList<UTF8String>();
        while (tokens.ttype != ')') {
            elements.add(UTF8String.fromString(tokens.sval));
            tokens.nextToken();
        }

        tokens.nextToken(); // consume )

        if (DataTypes.LongType.equals(dataType) || CachedArrayDataAccessor.LONG_ARRAY_TYPE.sameType(dataType)) {
            instructions.add(Instruction.load(Value.longArrayValue(elements.stream().mapToLong(UTF8String::toLongExact).toArray())));
        } else if (DataTypes.DoubleType.equals(dataType) || CachedArrayDataAccessor.DOUBLE_ARRAY_TYPE.sameType(dataType)) {
            instructions.add(Instruction.load(Value.doubleArrayValue(elements.stream().mapToDouble(element -> Double.parseDouble(element.toString())).toArray())));
        } else if (DataTypes.BooleanType.equals(dataType) || CachedArrayDataAccessor.BOOLEAN_ARRAY_TYPE.sameType(dataType)) {
            var booleans = new boolean[elements.size()];
            for (int i = 0; i < elements.size(); i++) {
                booleans[i] = Boolean.parseBoolean(elements.get(i).toString());
            }

            instructions.add(Instruction.load(Value.booleanArrayValue(booleans)));
        } else {
            instructions.add(Instruction.load(Value.stringArrayValue(elements.toArray(new UTF8String[0]))));
        }
    }

}
