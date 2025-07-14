package io.exsql.s3xty;

import com.google.common.base.Stopwatch;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeUnit;

public class S3Xty {
    public static void main(final String[] args) {
        var vm = new SExpressionVM();
        try {
            var program = Compiler.compile(StructType.fromDDL(args[0]), args[1]);

            var stopWatch = Stopwatch.createStarted();
            var row = new GenericRow(new Object[]{1L});
            for (int i = 0; i < 1000000; i++) {
                vm.reset();
                vm.evaluate(program, row);
            }

            System.out.println("(" + args[0] + ") " + args[1] + ": " + vm.result() + " took: " + stopWatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
        } catch (final Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}