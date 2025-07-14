package io.exsql.s3xty;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public final class Compiler {
    private Compiler() {}

    public static Program compile(final StructType schema, final String expression) throws IOException {
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
                    } else {
                        instructions.add(Instruction.equal());
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
            instructions.add(Instruction.load(Value.intValue(index)));
        } catch (final IllegalArgumentException illegalArgumentException) {
            dataType = DataTypes.StringType;
            instructions.add(Instruction.load(Value.fieldTypeValue(dataType)));
            instructions.add(Instruction.load(Value.intValue(-1)));
        }

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
        }

        instructions.add(Instruction.load(Value.stringValue(tokens.sval)));
    }

}
