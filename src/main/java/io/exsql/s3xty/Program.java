package io.exsql.s3xty;

import java.util.Iterator;
import java.util.List;

public final class Program implements Iterator<Instruction> {

    private final String expression;

    private final List<Instruction> instructions;

    private final Iterator<Instruction> iterator;

    Program(final String expression, final List<Instruction> instructions) {
        this.expression = expression;
        this.instructions = instructions;
        this.iterator = this.instructions.iterator();
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Instruction next() {
        return this.iterator.next();
    }

    @Override
    public String toString() {
        var instructionsToString = new StringBuilder();
        for (var instruction: this.instructions) {
            instructionsToString.append(instruction).append("\n\t\t\t");
        }

        return String.format(
                """
                    Program:
                        expression: %s
                        instructions:
                            %s
                """,
                this.expression,
                instructionsToString
        );
    }

}
