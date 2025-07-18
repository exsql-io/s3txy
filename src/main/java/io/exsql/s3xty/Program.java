package io.exsql.s3xty;

import java.util.List;

/**
 * Represents a program that can be executed by the VM.
 * A program consists of a list of instructions and the original expression.
 */
public final class Program {

    private final String expression;
    private final List<Instruction> instructions;
    private int currentIndex = 0;

    /**
     * Creates a new program with the given expression and instructions.
     *
     * @param expression the original expression
     * @param instructions the list of instructions
     * @throws IllegalArgumentException if the instructions list is null or empty
     */
    Program(final String expression, final List<Instruction> instructions) {
        if (instructions == null || instructions.isEmpty()) {
            throw new IllegalArgumentException("Instructions list cannot be null or empty");
        }
        this.expression = expression;
        this.instructions = List.copyOf(instructions);
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
    
    /**
     * Gets the total number of instructions in the program.
     *
     * @return the number of instructions
     */
    public int size() {
        return this.instructions.size();
    }

    @Override
    public String toString() {
        var instructionsToString = new StringBuilder();
        for (int i = 0; i < this.instructions.size(); i++) {
            instructionsToString.append(i).append(": ").append(this.instructions.get(i)).append("\n\t\t\t");
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
