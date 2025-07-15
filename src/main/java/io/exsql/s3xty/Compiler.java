package io.exsql.s3xty;

import com.google.common.base.Stopwatch;
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

public final class Compiler {

    private final static Logger LOGGER = LoggerFactory.getLogger(Compiler.class);

    private Compiler() {}

    public static Program compile(final StructType schema, final String expression) throws IOException {
        var stopWatch = Stopwatch.createStarted();
        var tokens = new StreamTokenizer(new StringReader(expression));

        var instructions = new ArrayList<Instruction>();
        while (tokens.nextToken() != StreamTokenizer.TT_EOF) {
            parseExpression(tokens, instructions, schema);
        }

        instructions.add(Instruction.halt());

        var elapsed = stopWatch.elapsed(TimeUnit.MILLISECONDS);
        LOGGER.info("compile phase took: {}ms", elapsed);

        return new Program(expression, instructions);
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
                parseExpression(tokens, instructions, schema);
                parseExpression(tokens, instructions, schema);
                instructions.add(Instruction.or());
                break;
            case Keywords.AND:
                tokens.nextToken(); // skip "and"
                parseExpression(tokens, instructions, schema);
                parseExpression(tokens, instructions, schema);
                instructions.add(Instruction.and());
                break;
            default:
                parseBinaryOperator(tokens, instructions, schema);
                break;
        }
        tokens.nextToken(); // skip )
    }

    private static void parseBinaryOperator(final StreamTokenizer tokens, final List<Instruction> instructions, final StructType schema) throws IOException {
        var operator = tokens.sval;

        tokens.nextToken(); // consume operator
        var dataType = parseGetField(tokens, instructions, schema); // parse the get field operation
        parseArgument(tokens, instructions, dataType); // parse the constant value to check against

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
        }
    }

    private static DataType parseGetField(final StreamTokenizer tokens, final List<Instruction> instructions, final StructType schema) throws IOException {
        DataType dataType;
        try {
            var index = schema.fieldIndex(tokens.sval);
            dataType = schema.apply(index).dataType();
            instructions.add(Instruction.load(Value.fieldTypeValue(dataType)));
        } catch (final IllegalArgumentException illegalArgumentException) {
            dataType = DataTypes.StringType;
            instructions.add(Instruction.load(Value.fieldTypeValue(dataType)));
        }

        instructions.add(Instruction.load(Value.stringValue(UTF8String.fromString(tokens.sval))));
        instructions.add(Instruction.getField());

        tokens.nextToken(); // consume get field operation

        return dataType;
    }

    private static void parseArgument(final StreamTokenizer tokens, final List<Instruction> instructions, final DataType dataType) throws IOException {
        if (dataType != null) {
            if (dataType.equals(DataTypes.LongType)) instructions.add(Instruction.load(Value.longValue(tokens.sval)));
            if (dataType.equals(DataTypes.DoubleType)) instructions.add(Instruction.load(Value.doubleValue(tokens.sval)));
            if (dataType.equals(DataTypes.BooleanType)) instructions.add(Instruction.load(Value.booleanValue(tokens.sval)));
            if (dataType.equals(DataTypes.StringType)) instructions.add(Instruction.load(Value.stringValue(UTF8String.fromString(tokens.sval))));
        } else {
            instructions.add(Instruction.load(Value.stringValue(UTF8String.fromString(tokens.sval))));
        }

        tokens.nextToken(); // consume argument
    }

}
