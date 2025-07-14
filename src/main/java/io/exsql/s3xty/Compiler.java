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
        tokens.ordinaryChar('(');
        tokens.ordinaryChar(')');

        var instructions = new ArrayList<Instruction>();
        while (tokens.nextToken() != StreamTokenizer.TT_EOF) {
            switch (tokens.ttype) {
                case '(':
                    parseExpression(tokens, instructions, schema);
                    break;
                case ')':
                    break;
            }
        }

        instructions.add(Instruction.halt());

        var elapsed = stopWatch.elapsed(TimeUnit.MILLISECONDS);
        LOGGER.info("compile phase took: {}ms", elapsed);

        return new Program(expression, instructions);
    }

    private static void parseExpression(final StreamTokenizer tokens, final List<Instruction> instructions, final StructType schema) throws IOException {
        switch (tokens.nextToken()) {
            case StreamTokenizer.TT_WORD:
                if ("trait-eq".equals(tokens.sval)) {
                    var dataType = parseGetField(tokens, instructions, schema); // parse the get field operation
                    parseArgument(tokens, instructions, dataType); // parse the constant value to check against

                    if (dataType.equals(DataTypes.LongType)) {
                        instructions.add(Instruction.longEqual());
                    } else if (dataType.equals(DataTypes.DoubleType)) {
                        instructions.add(Instruction.doubleEqual());
                    } else if (dataType.equals(DataTypes.BooleanType)) {
                        instructions.add(Instruction.booleanEqual());
                    } else {
                        instructions.add(Instruction.stringEqual());
                    }
                }
                break;
        }
    }

    private static DataType parseGetField(final StreamTokenizer tokens, final List<Instruction> instructions, final StructType schema) throws IOException {
        tokens.nextToken();

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

        return dataType;
    }

    private static void parseArgument(final StreamTokenizer tokens, final List<Instruction> instructions, final DataType dataType) throws IOException {
        tokens.nextToken();
        if (dataType != null) {
            if (dataType.equals(DataTypes.LongType)) {
                instructions.add(Instruction.load(Value.longValue(tokens.sval)));
                return;
            }

            if (dataType.equals(DataTypes.DoubleType)) {
                instructions.add(Instruction.load(Value.doubleValue(tokens.sval)));
                return;
            }

            if (dataType.equals(DataTypes.BooleanType)) {
                instructions.add(Instruction.load(Value.booleanValue(tokens.sval)));
                return;
            }
        }

        instructions.add(Instruction.load(Value.stringValue(UTF8String.fromString(tokens.sval))));
    }

}
