package io.exsql.s3xty;

import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SExpressionVMTest {

    private final SExpressionVM vm = new SExpressionVM();

    private final StructType schema = StructType.fromDDL("long LONG");

    @BeforeEach
    void reset() {
        vm.reset();
    }

    @Test
    void verifyTraitEq() throws IOException {
        vm.evaluate(Compiler.compile(schema, "(trait-eq \"long\" \"1\")"), new GenericRow(new Object[]{1L}));
        assertTrue(vm.result());
    }

}
