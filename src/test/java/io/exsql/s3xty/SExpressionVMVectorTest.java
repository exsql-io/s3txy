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
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SExpressionVMVectorTest {

    private final StructType schema = StructType.fromDDL("long LONG, double DOUBLE, boolean BOOLEAN");

    private final Map<String, String> environment = Map.of("S3XTY_VM_USE_VECTOR_API", "true");

    private final Object2ObjectOpenHashMap<UTF8String, DataType> fieldTypes = SchemaHelper.convert(schema);

    @Test
    void verifyTraitEqString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        var program = Compiler.compile(schema, "(trait-eq \"string\" \"string\")");
        var vm = new SExpressionVM(Map.of("S3XTY_VM_USE_VECTOR_API", "true"), program);
        vm.evaluate(bag);
        assertTrue(vm.result());
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
    void verifyTraitEqWithNullString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), null})
        }));

        var program = Compiler.compile(schema, "(trait-eq \"string\" \"test\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitEqWithEmptyString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        var program = Compiler.compile(schema, "(trait-eq \"string\" \"\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtWithEmptyString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        var program = Compiler.compile(schema, "(trait-lt \"string\" \"a\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("zebra")})
        }));

        var program = Compiler.compile(schema, "(trait-gt \"string\" \"apple\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtStringWithSameValue() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("apple")})
        }));

        var program = Compiler.compile(schema, "(trait-gt \"string\" \"apple\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLeString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("apple")})
        }));

        var program = Compiler.compile(schema, "(trait-le \"string\" \"apple\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeStringWithDifferentValues() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("apple")})
        }));

        var program = Compiler.compile(schema, "(trait-le \"string\" \"banana\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("banana")})
        }));

        var program = Compiler.compile(schema, "(trait-ge \"string\" \"banana\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeStringWithDifferentValues() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("banana")})
        }));

        var program = Compiler.compile(schema, "(trait-ge \"string\" \"apple\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqWithDifferentLengthStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string1"), UTF8String.fromString("short")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string2"), UTF8String.fromString("longer_string")})
        }));

        // First evaluation
        var program1 = Compiler.compile(schema, "(trait-eq \"string1\" \"short\")");
        var vm1 = new SExpressionVM(environment, program1);
        vm1.evaluate(bag);
        assertTrue(vm1.result());
        
        // Second evaluation
        var program2 = Compiler.compile(schema, "(trait-eq \"string2\" \"longer_string\")");
        var vm2 = new SExpressionVM(environment, program2);
        vm2.evaluate(bag);
        assertTrue(vm2.result());
    }

    @Test
    void verifyTraitLtWithCommonPrefixStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("prefix_a")})
        }));

        var program = Compiler.compile(schema, "(trait-lt \"string\" \"prefix_b\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqWithVeryLongStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("i am a really long string and I know you know it")})
        }));

        var program = Compiler.compile(schema, "(trait-eq \"string\" \"i am a really long string and I know you know it\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtWithVeryLongStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("i am a really long string and I know you know it")})
        }));

        var program = Compiler.compile(schema, "(trait-lt \"string\" \"i am too small\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeWithNullString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), null})
        }));

        var program = Compiler.compile(schema, "(trait-le \"string\" \"apple\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGtWithNullString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), null})
        }));

        var program = Compiler.compile(schema, "(trait-gt \"string\" \"apple\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGeWithNullString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), null})
        }));

        var program = Compiler.compile(schema, "(trait-ge \"string\" \"apple\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLeWithEmptyString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        var program = Compiler.compile(schema, "(trait-le \"string\" \"apple\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtWithEmptyString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        var program = Compiler.compile(schema, "(trait-gt \"string\" \"\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGeWithEmptyString() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        var program = Compiler.compile(schema, "(trait-ge \"string\" \"\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeWithDifferentLengthStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string1"), UTF8String.fromString("short")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string2"), UTF8String.fromString("longer_string")})
        }));

        // First evaluation
        var program1 = Compiler.compile(schema, "(trait-le \"string1\" \"short\")");
        var vm1 = new SExpressionVM(environment, program1);
        vm1.evaluate(bag);
        assertTrue(vm1.result());
        
        // Second evaluation
        var program2 = Compiler.compile(schema, "(trait-le \"string2\" \"z\")");
        var vm2 = new SExpressionVM(environment, program2);
        vm2.evaluate(bag);
        assertTrue(vm2.result());
    }

    @Test
    void verifyTraitGtWithDifferentLengthStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string1"), UTF8String.fromString("short")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string2"), UTF8String.fromString("longer_string")})
        }));

        // First evaluation
        var program1 = Compiler.compile(schema, "(trait-gt \"string1\" \"a\")");
        var vm1 = new SExpressionVM(environment, program1);
        vm1.evaluate(bag);
        assertTrue(vm1.result());
        
        // Second evaluation
        var program2 = Compiler.compile(schema, "(trait-gt \"string2\" \"a\")");
        var vm2 = new SExpressionVM(environment, program2);
        vm2.evaluate(bag);
        assertTrue(vm2.result());
    }

    @Test
    void verifyTraitGeWithDifferentLengthStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string1"), UTF8String.fromString("short")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string2"), UTF8String.fromString("longer_string")})
        }));

        // First evaluation
        var program1 = Compiler.compile(schema, "(trait-ge \"string1\" \"short\")");
        var vm1 = new SExpressionVM(environment, program1);
        vm1.evaluate(bag);
        assertTrue(vm1.result());
        
        // Second evaluation
        var program2 = Compiler.compile(schema, "(trait-ge \"string2\" \"longer\")");
        var vm2 = new SExpressionVM(environment, program2);
        vm2.evaluate(bag);
        assertTrue(vm2.result());
    }

    @Test
    void verifyTraitLeWithCommonPrefixStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("prefix_a")})
        }));

        var program = Compiler.compile(schema, "(trait-le \"string\" \"prefix_a\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtWithCommonPrefixStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("prefix_b")})
        }));

        var program = Compiler.compile(schema, "(trait-gt \"string\" \"prefix_a\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeWithCommonPrefixStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("prefix_a")})
        }));

        var program = Compiler.compile(schema, "(trait-ge \"string\" \"prefix_a\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeWithVeryLongStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("i am a really long string and I know you know it")})
        }));

        var program = Compiler.compile(schema, "(trait-le \"string\" \"i am a really long string and I know you know it\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtWithVeryLongStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("z am a really long string and I know you know it")})
        }));

        var program = Compiler.compile(schema, "(trait-gt \"string\" \"a am a really long string and I know you know it\")");
        var vm = new SExpressionVM(environment, program);
        vm.evaluate(bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeWithVeryLongStrings() {
        var bag = TraitAccessor.forArrayData(fieldTypes, ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("i am a really long string and I know you know it")})
        }));

        var program = Compiler.compile(schema, "(trait-ge \"string\" \"i am a really long string and I know you know it\")");
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

}
