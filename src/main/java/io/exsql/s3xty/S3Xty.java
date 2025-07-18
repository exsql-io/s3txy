package io.exsql.s3xty;

import com.google.common.base.Stopwatch;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class S3Xty {

    private final static Logger LOGGER = LoggerFactory.getLogger(S3Xty.class);

    public static void main(final String[] args) {
        var vm = new SExpressionVM();
        try {
            var program = Compiler.compile(StructType.fromDDL(args[0]), args[1]);
            LOGGER.info("\n{}", program);

            var bag = new CachedValueBag(ArrayData.toArrayData(new GenericInternalRow[] {
                    new GenericInternalRow(new Object[]{UTF8String.fromString("long"), UTF8String.fromString("1")})
            }));

            var stopWatch = Stopwatch.createStarted();
            for (int i = 0; i < 1_000_000_000; i++) {
                vm.reset();
                vm.evaluate(program, bag);
            }

            var elapsed = stopWatch.elapsed(TimeUnit.MILLISECONDS);
            LOGGER.info("result: {}", vm.result());
            LOGGER.info("evaluation took: {}ms", elapsed);
        } catch (final Throwable throwable) {
            LOGGER.error("An error occurred while evaluating expression. See logs for more details.", throwable);
        }
    }

}