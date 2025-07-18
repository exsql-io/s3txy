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

}
