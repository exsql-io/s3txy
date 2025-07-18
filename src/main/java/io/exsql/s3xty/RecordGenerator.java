package io.exsql.s3xty;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Random;

public final class RecordGenerator {

    private final static Random RANDOM = new Random(991512);

    private final static String[] STRING_VALUES = {
            "test", "positive", "value", "", "hello", "world", "boundary",
            "mixed", "answer", "a", "b", "false", "extreme", "outlier",
            "valid", "zero", "special", "first", "second", "third",
            "percentage", "yes", "true", "unlucky", "pi", "correct",
            "normal", "four-digit", "allowed", "invalid", "positive-double",
            "option1", "option2", "option3", "fraction", "significant",
            "lucky", "ten", "match", "sequence", "byte", "permitted",
            "reject", "high-value", "adult", "red", "green", "blue",
            "half-or-less", "negative-valid", "far-from-zero", "ok",
            "devil", "e", "exact", "circle", "moderate", "month", "accepted"
    };

    public static CachedValueBag[] generate(final int count) {
        var bags = new CachedValueBag[count];
        for (var i = 0; i < count; i++) {
            // Decide how many fields to include (1-4)
            int fieldCount = 1 + RANDOM.nextInt(4);

            // Track which fields we've already added
            boolean hasLong = false;
            boolean hasDouble = false;
            boolean hasBoolean = false;
            boolean hasString = false;

            var entries = new GenericInternalRow[fieldCount];
            for (int j = 0; j < fieldCount; j++) {
                // Choose which field to add next
                int fieldType = RANDOM.nextInt(4);

                // If we've already added this field type, try another one
                while ((fieldType == 0 && hasLong) ||
                        (fieldType == 1 && hasDouble) ||
                        (fieldType == 2 && hasBoolean) ||
                        (fieldType == 3 && hasString)) {
                    fieldType = RANDOM.nextInt(4);
                }

                // Create the row based on field type
                switch (fieldType) {
                    case 0: // long
                        hasLong = true;
                        // Generate values that could match expressions like (trait-eq "long" "42"), (trait-gt "long" "10"), etc.
                        long longValue;
                        int longChoice = RANDOM.nextInt(10);
                        if (longChoice == 0) longValue = 0;
                        else if (longChoice == 1) longValue = 1;
                        else if (longChoice == 2) longValue = 42;
                        else if (longChoice == 3) longValue = RANDOM.nextInt(10) + 10; // 10-19
                        else if (longChoice == 4) longValue = RANDOM.nextInt(50) + 50; // 50-99
                        else if (longChoice == 5) longValue = RANDOM.nextInt(900) + 100; // 100-999
                        else if (longChoice == 6) longValue = RANDOM.nextInt(9000) + 1000; // 1000-9999
                        else if (longChoice == 7) longValue = -RANDOM.nextInt(100); // -1 to -100
                        else if (longChoice == 8) longValue = -RANDOM.nextInt(900) - 100; // -100 to -999
                        else longValue = -RANDOM.nextInt(9000) - 1000; // -1000 to -9999

                        entries[j] = new GenericInternalRow(new Object[]{
                                UTF8String.fromString("long"),
                                UTF8String.fromString(String.valueOf(longValue))
                        });
                        break;

                    case 1: // double
                        hasDouble = true;
                        // Generate values that could match expressions like (trait-eq "double" "3.14"), (trait-gt "double" "0.0"), etc.
                        double doubleValue;
                        int doubleChoice = RANDOM.nextInt(10);
                        if (doubleChoice == 0) doubleValue = 0.0;
                        else if (doubleChoice == 1) doubleValue = 1.0;
                        else if (doubleChoice == 2) doubleValue = 3.14;
                        else if (doubleChoice == 3) doubleValue = 5.5;
                        else if (doubleChoice == 4) doubleValue = 10.0;
                        else if (doubleChoice == 5) doubleValue = RANDOM.nextDouble(); // 0.0-1.0
                        else if (doubleChoice == 6) doubleValue = RANDOM.nextDouble() * 10; // 0.0-10.0
                        else if (doubleChoice == 7) doubleValue = RANDOM.nextDouble() * 100; // 0.0-100.0
                        else if (doubleChoice == 8) doubleValue = -RANDOM.nextDouble() * 10; // -0.0 to -10.0
                        else doubleValue = -RANDOM.nextDouble() * 100; // -0.0 to -100.0

                        entries[j] = new GenericInternalRow(new Object[]{
                                UTF8String.fromString("double"),
                                UTF8String.fromString(String.valueOf(doubleValue))
                        });
                        break;

                    case 2: // boolean
                        hasBoolean = true;
                        // Generate values that could match expressions like (trait-eq "boolean" "true"), (trait-ne "boolean" "false"), etc.
                        boolean boolValue = RANDOM.nextBoolean();
                        entries[j] = new GenericInternalRow(new Object[]{
                                UTF8String.fromString("boolean"),
                                UTF8String.fromString(String.valueOf(boolValue))
                        });
                        break;

                    case 3: // string
                        hasString = true;
                        // Choose from predefined strings that appear in the expressions
                        String stringValue = STRING_VALUES[RANDOM.nextInt(STRING_VALUES.length)];
                        entries[j] = new GenericInternalRow(new Object[]{
                                UTF8String.fromString("string"),
                                UTF8String.fromString(stringValue)
                        });
                        break;
                }
            }

            bags[i] = new CachedValueBag(ArrayData.toArrayData(entries));
        }

        return bags;
    }

}
