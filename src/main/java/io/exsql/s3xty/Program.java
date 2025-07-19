package io.exsql.s3xty;

import java.util.List;

/**
 * Represents a program that can be executed by the VM.
 * A program consists of a list of instructions and the original expression.
 */
public final class Program {

    private final String[] expressions;
    private final List<Instruction> instructions;
    private int currentIndex = 0;

    /**
     * Creates a new program with the given expressions and instructions.
     *
     * @param expressions the original expressions
     * @param instructions the list of instructions
     * @throws IllegalArgumentException if the instructions list is null or empty
     */
    Program(final String[] expressions, final List<Instruction> instructions) {
        if (instructions == null || instructions.isEmpty()) {
            throw new IllegalArgumentException("Instructions list cannot be null or empty");
        }
        this.expressions = expressions;
        this.instructions = List.copyOf(instructions);
    }

    public Program fork() {
        return new Program(this.expressions, this.instructions);
    }

    /**
     * Returns whether there are more instructions to execute.
     *
     * @return true if there are more instructions, false otherwise
     */
    public boolean hasNext() {
        return this.currentIndex < this.instructions.size();
    }

    /**
     * Returns the next instruction and advances the current index.
     *
     * @return the next instruction
     * @throws IndexOutOfBoundsException if there are no more instructions
     */
    public Instruction next() {
        if (!hasNext()) {
            throw new IndexOutOfBoundsException("No more instructions");
        }
        return this.instructions.get(currentIndex++);
    }
    
    /**
     * Gets the current instruction index.
     *
     * @return the current instruction index
     */
    public int getCurrentIndex() {
        return this.currentIndex;
    }
    
    /**
     * Sets the current instruction index.
     *
     * @param index the new instruction index
     * @throws IndexOutOfBoundsException if the index is out of bounds
     */
    public void setCurrentIndex(int index) {
        if (index < 0 || index >= this.instructions.size()) {
            throw new IndexOutOfBoundsException("Invalid instruction index: " + index);
        }
        this.currentIndex = index;
    }

    public boolean[] output() {
        return new boolean[this.expressions.length];
    }

    @Override
    public String toString() {
        var expressionsToString = new StringBuilder();
        for (int i = 0; i < this.expressions.length; i++) {
            expressionsToString.append(i).append(": ").append(this.expressions[i]).append("\n\t\t\t");
        }

        var instructionsToString = new StringBuilder();
        for (int i = 0; i < this.instructions.size(); i++) {
            instructionsToString.append(i).append(": ").append(this.instructions.get(i)).append("\n\t\t\t");
        }

        return String.format(
                """
                    Program:
                        expressions:
                            %s
                        instructions:
                            %s
                """,
                expressionsToString,
                instructionsToString
        );
    }

}
