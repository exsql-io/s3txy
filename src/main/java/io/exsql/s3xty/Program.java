package io.exsql.s3xty;

import java.util.Iterator;
import java.util.List;

public final class Program implements Iterator<Instruction> {

    private final String expression;

    private final Iterator<Instruction> instructions;

    Program(final String expression, final List<Instruction> instructions) {
        this.expression = expression;
        this.instructions = instructions.iterator();
    }

    @Override
    public boolean hasNext() {
        return this.instructions.hasNext();
    }

    public Instruction next() {
        return this.instructions.next();
    }

}
