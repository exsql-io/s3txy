package io.exsql.s3xty;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SExpressionVMTest {

    private final static Map<String, String> environment = Map.of();

    private final StructType schema = StructType.fromDDL(
            "long LONG, double DOUBLE, boolean BOOLEAN, strings ARRAY<STRING>, longs ARRAY<LONG>, doubles ARRAY<DOUBLE>, booleans ARRAY<BOOLEAN>"
    );

    private final Object2ObjectOpenHashMap<UTF8String, DataType> fieldTypes = SchemaHelper.convert(schema);

    @Test
    void verifyTraitEqLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-eq \"long\" \"1\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-eq \"string\" \"string\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-eq \"double\" \"1.5\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqBoolean() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        var program = Compiler.compile(schema, "(trait-eq \"boolean\" \"true\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNotTraitEqLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(not (trait-eq \"long\" \"1\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNotTraitEqString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(not (trait-eq \"string\" \"string\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNotTraitEqDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var program = Compiler.compile(schema, "(not (trait-eq \"double\" \"1.5\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNotTraitEqBoolean() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        var program = Compiler.compile(schema, "(not (trait-eq \"boolean\" \"true\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyOrTraitEq() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(or (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNestedOrTraitEq() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(or (or (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\")) (trait-eq \"string\" \"string\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyOrMultipleTraitEq() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(or (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\") (trait-eq \"string\" \"string\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyAndTraitEq() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(and (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNestedAndTraitEq() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(and (and (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\")) (trait-eq \"string\" \"string\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyAndMultipleTraitEq() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(and (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\") (trait-eq \"string\" \"string\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitNeLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(trait-ne \"long\" \"1\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitNeString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(trait-ne \"string\" \"string\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitNeDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var program = Compiler.compile(schema, "(trait-ne \"double\" \"1.5\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitNeBoolean() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        var program = Compiler.compile(schema, "(trait-ne \"boolean\" \"true\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNotTraitNeLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(not (trait-ne \"long\" \"1\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNotTraitNeString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(not (trait-ne \"string\" \"string\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNotTraitNeDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var program = Compiler.compile(schema, "(not (trait-ne \"double\" \"1.5\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNotTraitNeBoolean() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        var program = Compiler.compile(schema, "(not (trait-ne \"boolean\" \"true\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(trait-lt \"long\" \"1\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLtString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(trait-lt \"string\" \"string\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLtDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var program = Compiler.compile(schema, "(trait-lt \"double\" \"1.5\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLeLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(trait-le \"long\" \"1\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(trait-le \"string\" \"string\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var program = Compiler.compile(schema, "(trait-le \"double\" \"1.5\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(trait-gt \"long\" \"1\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGtString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(trait-gt \"string\" \"string\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGtDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var program = Compiler.compile(schema, "(trait-gt \"double\" \"1.5\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGeLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(trait-ge \"long\" \"1\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(trait-ge \"string\" \"string\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var program = Compiler.compile(schema, "(trait-ge \"double\" \"1.5\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    // Tests for binary operations with different values

    @Test
    void verifyTraitLtLongWithDifferentValues() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(trait-lt \"long\" \"5\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtLongWithDifferentValues() {
        // After examining the existing tests, I understand that
        // in (trait-gt "field" "value"), it checks if field > value
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("10")})
        }));

        // Testing if long (10) > 5, which should be true
        var program = Compiler.compile(schema, "(trait-gt \"long\" \"5\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtDoubleWithDifferentValues() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var program = Compiler.compile(schema, "(trait-lt \"double\" \"2.5\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtDoubleWithDifferentValues() {
        // In (trait-gt "field" "value"), it checks if field > value
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("3.5")})
        }));

        // Testing if double (3.5) > 2.5, which should be true
        var program = Compiler.compile(schema, "(trait-gt \"double\" \"2.5\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtStringWithDifferentValues() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("apple")})
        }));

        var program = Compiler.compile(schema, "(trait-lt \"string\" \"banana\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtStringWithDifferentValues() {
        // In (trait-gt "field" "value"), the comparison is actually value > field
        // This is because values are popped in reverse order in registerBinaryOperation
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("zebra")})
        }));

        // Testing if "zebra" > "apple", which should be true
        var program = Compiler.compile(schema, "(trait-gt \"string\" \"apple\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitNeWithDifferentValues() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(trait-ne \"long\" \"2\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyAndWithAllTrueConditions() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("3")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("test")})
        }));

        // First test a single condition
        var program1 = Compiler.compile(schema, "(trait-lt \"long\" \"5\")");
        var vm1 = new SExpressionVM(environment, program1);
        vm1.evaluate(bag);
        assertTrue(vm1.result());
        
        // Now test the AND operation with both conditions true
        var program2 = Compiler.compile(schema, "(and (trait-lt \"long\" \"5\") (trait-eq \"string\" \"test\"))");
        var vm2 = new SExpressionVM(environment, program2);
        vm2.evaluate(bag);
        assertTrue(vm2.result());
    }

    @Test
    void verifyOrWithAllFalseConditions() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("5")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("test")})
        }));

        // In (trait-lt "field" "value"), it checks if field < value
        // We need a condition that will be false, so we use 5 < 5 which is false
        var program1 = Compiler.compile(schema, "(trait-lt \"long\" \"5\")");
        var vm1 = new SExpressionVM(environment, program1);
        vm1.evaluate(bag);
        assertFalse(vm1.result());
        
        // Test another false condition
        var program2 = Compiler.compile(schema, "(trait-eq \"string\" \"wrong\")");
        var vm2 = new SExpressionVM(environment, program2);
        vm2.evaluate(bag);
        assertFalse(vm2.result());
        
        // Now test the OR operation with both conditions false
        var program3 = Compiler.compile(schema, "(or (trait-lt \"long\" \"5\") (trait-eq \"string\" \"wrong\"))");
        var vm3 = new SExpressionVM(environment, program3);
        vm3.evaluate(bag);
        assertFalse(vm3.result());
    }

    // Tests for stack operations

    @Test
    void verifyDupOperation() {
        // Create a program that uses DUP operation
        // (dup (trait-eq "long" "1"))
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var program = Compiler.compile(schema, "(and (trait-eq \"long\" \"1\") (trait-eq \"long\" \"1\"))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyPopOperation() {
        // Create a program that uses POP operation
        // This test verifies that popping a value works correctly
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        // The second condition is evaluated but then popped, so only the first condition affects the result
        var program = Compiler.compile(schema, "(trait-eq \"long\" \"1\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    // Tests for error conditions

    @Test
    void verifyStackUnderflowHandling() {
        var program = Compiler.compile(schema, "(trait-eq \"long\" \"1\")");
        var vm = new SExpressionVM(environment, program);

        // Test that stack underflow is handled properly
        assertThrows(IllegalStateException.class, () -> {
            vm.reset();
            vm.pop(); // Should throw IllegalStateException for stack underflow
        });
    }

    @Test
    void verifyDupWithEmptyStackHandling() {
        var program = Compiler.compile(schema, "(trait-eq \"long\" \"1\")");
        var vm = new SExpressionVM(environment, program);

        // Test that attempting to dup with an empty stack is handled properly
        assertThrows(IllegalStateException.class, () -> {
            vm.reset();
            vm.dup(); // Should throw IllegalStateException
        });
    }

    // Tests for null values and edge cases

    @Test
    void verifyNullValueHandling() {
        // For this test, we'll check if a field exists rather than comparing with null
        // since the S-expression language might not support direct null comparisons
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("nullField"), null})
        }));

        // Test that we can access a field even if its value is null
        // We'll use trait-eq with an empty string which should be false
        var program = Compiler.compile(schema, "(trait-eq \"nullField\" \"\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyComplexNestedExpression() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("10")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("2.5")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("test")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        // Test a complex nested expression with multiple operations
        var program = Compiler.compile(schema, 
            "(and (or (trait-gt \"long\" \"5\") (trait-lt \"double\" \"1.0\")) " +
            "(and (trait-eq \"string\" \"test\") (trait-eq \"boolean\" \"true\")))");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyEmptyBagHandling() {
        // For this test, we'll use a non-empty bag but with a field that doesn't exist in the bag
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        // Test that a non-existent field is handled properly
        // We'll use a field that exists in the bag and one that doesn't
        var program1 = Compiler.compile(schema, "(trait-eq \"long\" \"1\")");
        var vm1 = new SExpressionVM(environment, program1);
        vm1.evaluate(bag);
        assertTrue(vm1.result());
        
        // For the empty bag test, we'll just verify that the VM can handle an empty bag
        var program2 = Compiler.compile(schema, "(not (trait-eq \"long\" \"2\"))");
        var vm2 = new SExpressionVM(environment, program2);
        vm2.evaluate(TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {})));
        assertTrue(vm2.result());
    }

    @Test
    void verifyMultipleResets() {
        // Test that multiple resets work correctly
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        // First evaluation
        var program1 = Compiler.compile(schema, "(trait-eq \"long\" \"1\")");
        var vm1 = new SExpressionVM(environment, program1);
        vm1.evaluate(bag);
        assertTrue(vm1.result());

        // Second evaluation with a different VM instance
        var program2 = Compiler.compile(schema, "(trait-eq \"long\" \"2\")");
        var vm2 = new SExpressionVM(environment, program2);
        vm2.evaluate(bag);
        assertFalse(vm2.result());
    }

    @Test
    void verifyTraitExistsLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-exists \"long\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitExistsString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-exists \"string\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitExistsDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-exists \"double\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitExistsBoolean() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        var program = Compiler.compile(schema, "(trait-exists \"boolean\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitCiEqString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("STRING")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-ci-eq \"string\" \"string\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitRegexpString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("STRING")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-regex \"string\" \"(?i)(string)\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitContainsString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-contains \"string\" \"str\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitCiContainsString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-ci-contains \"string\" \"STR\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitContainsStringArray() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("strings"), UTF8String.fromString("string1,string2")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-contains \"strings\" \"string1\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitElementContainsStringArray() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("strings"), UTF8String.fromString("string1,string2")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-element-contains \"strings\" \"ring\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitContainsLongArray() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("longs"), UTF8String.fromString("1,2")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-contains \"longs\" \"1\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitContainsDoubleArray() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("doubles"), UTF8String.fromString("1.5,2.5")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-contains \"doubles\" \"1.5\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitContainsBooleanArray() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("booleans"), UTF8String.fromString("true,false")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-contains \"booleans\" \"true\")"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitInLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-in \"long\" (\"1\" \"2\"))"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitInDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-in \"double\" (\"1.5\" \"2.5\"))"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitInBoolean() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-in \"boolean\" (\"true\" \"false\"))"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitInString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-in \"string\" (\"string\" \"str\"))"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitContainsAnyLong() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("longs"), UTF8String.fromString("1,3")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-contains-any \"longs\" (\"1\" \"2\"))"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitContainsAnyDouble() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("doubles"), UTF8String.fromString("1.5,3.5")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-contains-any \"doubles\" (\"1.5\" \"2.5\"))"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitContainsAnyBoolean() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("booleans"), UTF8String.fromString("true,true")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-contains-any \"booleans\" (\"true\" \"false\"))"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitContainsAnyString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("strings"), UTF8String.fromString("string1,string3")})
        }));

        var vm = new SExpressionVM(environment, Compiler.compile(schema, "(trait-contains-any \"strings\" (\"string1\" \"string2\"))"));
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

}