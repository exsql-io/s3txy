package io.exsql.s3xty;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SExpressionVMVectorTest {

    private final SExpressionVM vm = new SExpressionVM(Map.of("S3XTY_VM_USE_VECTOR_API", "true"));

    private final StructType schema = StructType.fromDDL("long LONG, double DOUBLE, boolean BOOLEAN");

    @BeforeEach
    void reset() {
        vm.reset();
    }

    @Test
    void verifyTraitEqString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-eq \"string\" \"string\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNotTraitEqString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-eq \"string\" \"string\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNestedOrTraitEq() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(or (or (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\")) (trait-eq \"string\" \"string\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyOrMultipleTraitEq() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(or (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\") (trait-eq \"string\" \"string\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNestedAndTraitEq() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(and (and (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\")) (trait-eq \"string\" \"string\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyAndMultipleTraitEq() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(and (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\") (trait-eq \"string\" \"string\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitNeString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ne \"string\" \"string\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNotTraitNeString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-ne \"string\" \"string\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyAndWithAllTrueConditions() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("3")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("test")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-lt \"long\" \"5\")"), bag);
        assertTrue(vm.result());
        
        // Now test the AND operation with both conditions true
        vm.reset();
        vm.evaluate(Compiler.compile(schema, "(and (trait-lt \"long\" \"5\") (trait-eq \"string\" \"test\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyOrWithAllFalseConditions() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("5")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("test")})
        }));

        // In (trait-lt "field" "value"), it checks if field < value
        // We need a condition that will be false, so we use 5 < 5 which is false
        vm.evaluate(Compiler.compile(schema, "(trait-lt \"long\" \"5\")"), bag);
        assertFalse(vm.result());
        
        vm.reset();
        vm.evaluate(Compiler.compile(schema, "(trait-eq \"string\" \"wrong\")"), bag);
        assertFalse(vm.result());
        
        // Now test the OR operation with both conditions false
        vm.reset();
        vm.evaluate(Compiler.compile(schema, "(or (trait-lt \"long\" \"5\") (trait-eq \"string\" \"wrong\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyComplexNestedExpression() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("10")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("2.5")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("test")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        // Test a complex nested expression with multiple operations
        vm.evaluate(Compiler.compile(schema, 
            "(and (or (trait-gt \"long\" \"5\") (trait-lt \"double\" \"1.0\")) " +
            "(and (trait-eq \"string\" \"test\") (trait-eq \"boolean\" \"true\")))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-lt \"string\" \"string\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLtStringWithDifferentValues() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("apple")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-lt \"string\" \"banana\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqWithNullString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), null})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-eq \"string\" \"test\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitEqWithEmptyString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-eq \"string\" \"\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtWithEmptyString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-lt \"string\" \"a\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("zebra")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"string\" \"apple\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtStringWithSameValue() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("apple")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"string\" \"apple\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLeString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("apple")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"string\" \"apple\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeStringWithDifferentValues() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("apple")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"string\" \"banana\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("banana")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"string\" \"banana\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeStringWithDifferentValues() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("banana")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"string\" \"apple\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqWithDifferentLengthStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string1"), UTF8String.fromString("short")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string2"), UTF8String.fromString("longer_string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-eq \"string1\" \"short\")"), bag);
        assertTrue(vm.result());
        
        vm.reset();
        vm.evaluate(Compiler.compile(schema, "(trait-eq \"string2\" \"longer_string\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtWithCommonPrefixStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("prefix_a")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-lt \"string\" \"prefix_b\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqWithVeryLongStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("i am a really long string and I know you know it")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-eq \"string\" \"i am a really long string and I know you know it\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtWithVeryLongStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("i am a really long string and I know you know it")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-lt \"string\" \"i am too small\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeWithNullString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), null})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"string\" \"apple\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGtWithNullString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), null})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"string\" \"apple\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeWithNullString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), null})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"string\" \"apple\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeWithEmptyString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"string\" \"apple\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtWithEmptyString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"string\" \"\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGeWithEmptyString() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"string\" \"\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeWithDifferentLengthStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string1"), UTF8String.fromString("short")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string2"), UTF8String.fromString("longer_string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"string1\" \"short\")"), bag);
        assertTrue(vm.result());
        
        vm.reset();
        vm.evaluate(Compiler.compile(schema, "(trait-le \"string2\" \"z\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtWithDifferentLengthStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string1"), UTF8String.fromString("short")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string2"), UTF8String.fromString("longer_string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"string1\" \"a\")"), bag);
        assertTrue(vm.result());
        
        vm.reset();
        vm.evaluate(Compiler.compile(schema, "(trait-gt \"string2\" \"a\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeWithDifferentLengthStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string1"), UTF8String.fromString("short")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string2"), UTF8String.fromString("longer_string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"string1\" \"short\")"), bag);
        assertTrue(vm.result());
        
        vm.reset();
        vm.evaluate(Compiler.compile(schema, "(trait-ge \"string2\" \"longer\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeWithCommonPrefixStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("prefix_a")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"string\" \"prefix_a\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtWithCommonPrefixStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("prefix_b")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"string\" \"prefix_a\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeWithCommonPrefixStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("prefix_a")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"string\" \"prefix_a\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeWithVeryLongStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("i am a really long string and I know you know it")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"string\" \"i am a really long string and I know you know it\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtWithVeryLongStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("z am a really long string and I know you know it")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"string\" \"a am a really long string and I know you know it\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeWithVeryLongStrings() {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("i am a really long string and I know you know it")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"string\" \"i am a really long string and I know you know it\")"), bag);
        assertTrue(vm.result());
    }
}
