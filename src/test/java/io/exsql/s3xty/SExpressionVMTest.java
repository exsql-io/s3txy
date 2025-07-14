package io.exsql.s3xty;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

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

}
