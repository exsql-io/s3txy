package io.exsql.s3xty;

import java.io.*;

/**
 * Represents a program that can be executed by the VM.
 * A program consists of a list of instructions and the original expression.
 */
public final class Program implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String[] expressions;
    private Instruction[] instructions;
    private transient int currentIndex = 0;

    /**
     * Creates a new program with the given expressions and instructions.
     *
     * @param expressions the original expressions
     * @param instructions the list of instructions
     * @throws IllegalArgumentException if the instructions list is null or empty
     */
    Program(final String[] expressions, final Instruction[] instructions) {
        if (instructions == null || instructions.length == 0) {
            throw new IllegalArgumentException("Instructions list cannot be null or empty");
        }

        this.expressions = expressions;
        this.instructions = instructions;
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
        return this.currentIndex < this.instructions.length;
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
        return this.instructions[currentIndex++];
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
        if (index < 0 || index >= this.instructions.length) {
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
        for (int i = 0; i < this.instructions.length; i++) {
            instructionsToString.append(i).append(": ").append(this.instructions[i]).append("\n\t\t\t");
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

    @Serial
    private void writeObject(final ObjectOutputStream oos) throws IOException {
        oos.writeObject(this.expressions);
        oos.writeObject(this.instructions);
    }

    @Serial
    private void readObject(final ObjectInputStream ois) throws ClassNotFoundException, IOException {
        this.expressions = (String[]) ois.readObject();
        this.instructions = (Instruction[]) ois.readObject();
        this.currentIndex = 0;
    }

}
