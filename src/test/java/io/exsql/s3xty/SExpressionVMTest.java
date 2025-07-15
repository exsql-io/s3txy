package io.exsql.s3xty;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SExpressionVMTest {

    private final SExpressionVM vm = new SExpressionVM();

    private final StructType schema = StructType.fromDDL("long LONG, double DOUBLE, boolean BOOLEAN");

    @BeforeEach
    void reset() {
        vm.reset();
    }

    @Test
    void verifyTraitEqLong() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-eq \"long\" \"1\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqString() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-eq \"string\" \"string\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqDouble() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-eq \"double\" \"1.5\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitEqBoolean() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-eq \"boolean\" \"true\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNotTraitEqLong() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-eq \"long\" \"1\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNotTraitEqString() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-eq \"string\" \"string\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNotTraitEqDouble() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-eq \"double\" \"1.5\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNotTraitEqBoolean() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-eq \"boolean\" \"true\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyOrTraitEq() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(or (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNestedOrTraitEq() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(or (or (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\")) (trait-eq \"string\" \"string\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyAndTraitEq() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(and (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNestedAndTraitEq() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")}),
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(and (and (trait-eq \"boolean\" \"true\") (trait-eq \"long\" \"5\")) (trait-eq \"string\" \"string\"))"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitNeLong() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ne \"long\" \"1\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitNeString() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ne \"string\" \"string\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitNeDouble() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ne \"double\" \"1.5\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitNeBoolean() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ne \"boolean\" \"true\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyNotTraitNeLong() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-ne \"long\" \"1\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNotTraitNeString() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-ne \"string\" \"string\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNotTraitNeDouble() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-ne \"double\" \"1.5\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyNotTraitNeBoolean() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("boolean"), UTF8String.fromString("true")})
        }));

        vm.evaluate(Compiler.compile(schema, "(not (trait-ne \"boolean\" \"true\"))"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLtLong() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-lt \"long\" \"1\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLtString() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-lt \"string\" \"string\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLtDouble() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-lt \"double\" \"1.5\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitLeLong() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"long\" \"1\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeString() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"string\" \"string\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitLeDouble() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-le \"double\" \"1.5\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGtLong() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"long\" \"1\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGtString() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"string\" \"string\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGtDouble() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-gt \"double\" \"1.5\")"), bag);
        assertFalse(vm.result());
    }

    @Test
    void verifyTraitGeLong() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"long\" \"1\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeString() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("string"), UTF8String.fromString("string")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"string\" \"string\")"), bag);
        assertTrue(vm.result());
    }

    @Test
    void verifyTraitGeDouble() throws IOException {
        var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                new GenericInternalRow(new Object[]{UTF8String.fromString("double"), UTF8String.fromString("1.5")})
        }));

        vm.evaluate(Compiler.compile(schema, "(trait-ge \"double\" \"1.5\")"), bag);
        assertTrue(vm.result());
    }

}
