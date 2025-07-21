package io.exsql.s3xty;

import com.google.common.base.Stopwatch;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class S3Xty {

    private final static Logger LOGGER = LoggerFactory.getLogger(S3Xty.class);

    private final static AtomicLong totalActualTime = new AtomicLong(0);

    public static void main(final String[] args) {
        try {
            var evaluations = Integer.parseInt(args[0]);
            var records = Integer.parseInt(args[1]);

            LOGGER.info("Generating {} records for evaluation", records);
            var data = RecordGenerator.generate(records);

            LOGGER.info("Records generated");

            LOGGER.info("Using schema: {}", args[2]);
            var schema = StructType.fromDDL(args[2]);

            var expressions = new String[args.length - 3];
            System.arraycopy(args, 3, expressions, 0, expressions.length);

            var program = Compiler.compile(schema, expressions);
            LOGGER.debug("\n{}", program);

            var threads = new ArrayList<Thread>();

            LOGGER.info("Starting evaluation of {} expressions", expressions.length);
            var globalStopWatch = Stopwatch.createStarted();
            for (var evaluation = 0; evaluation < evaluations; evaluation++) {
                var thread = new Thread(createTask(evaluation, program.fork(), initializeBags(data)));
                thread.start();
                threads.add(thread);
            }

            for (var thread: threads) {
                thread.join();
            }

            LOGGER.info(
                    "evaluating {} expressions on {} profiles {} times took: {}s (actual: {}ms)",
                    expressions, records, evaluations, globalStopWatch.elapsed(TimeUnit.SECONDS), totalActualTime.get()
            );
        } catch (final Throwable throwable) {
            LOGGER.error("An error occurred while evaluating expression. See logs for more details.", throwable);
        }
    }

    private static @NotNull Runnable createTask(final int evaluation, final Program program, final CachedValueBag[] bags) {
        return () -> {
            var vm = new SExpressionVM(System.getenv(), program);
            var stopWatch = Stopwatch.createStarted();
            for (var bag: bags) {
                vm.reset();
                vm.evaluate(bag);
            }

            var elapsed = stopWatch.elapsed(TimeUnit.MILLISECONDS);
            totalActualTime.addAndGet(elapsed);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("result: {}", Arrays.toString(vm.results()));
            }

            LOGGER.info("evaluating #{} took: {}ms", evaluation, elapsed);
        };
    }

    private static CachedValueBag[] initializeBags(final ArrayData[] data) {
        var bags = new CachedValueBag[data.length];
        for (var i = 0; i < data.length; i++) {
            bags[i] = new CachedValueBag(data[i]);
        }
        return bags;
    }

}