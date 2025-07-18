package io.exsql.s3xty;

import com.google.common.base.Stopwatch;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class S3Xty {

    private final static Logger LOGGER = LoggerFactory.getLogger(S3Xty.class);

    public static void main(final String[] args) {
        var threads = new ArrayList<Thread>();
        try {
            var evaluations = Integer.parseInt(args[0]);
            var records = Integer.parseInt(args[1]);

            LOGGER.info("Generating {} records for evaluation", records);
            var bags = RecordGenerator.generate(records);
            LOGGER.info("Records generated");

            LOGGER.info("Using schema: {}", args[2]);
            var schema = StructType.fromDDL(args[2]);

            var globalStopWatch = Stopwatch.createStarted();

            var expressions = args.length - 3;
            LOGGER.info("Starting evaluation of {} expressions", expressions);
            for (var i = 3; i < args.length; i++) {
                final int index = i;
                final Runnable task = () -> {
                    var vm = new SExpressionVM(System.getenv());
                    var program = Compiler.compile(schema, args[index]);
                    LOGGER.debug("\n{}", program);

                    var stopWatch = Stopwatch.createStarted();
                    for (var j = 0; j < evaluations; j++) {
                        for (var bag: bags) {
                            vm.reset();
                            vm.evaluate(program, bag);
                        }
                    }

                    var elapsed = stopWatch.elapsed(TimeUnit.MILLISECONDS);
                    LOGGER.debug("result: {}", vm.result());
                    LOGGER.info("evaluating {} took: {}ms", args[index], elapsed);
                };

                threads.add(Thread.ofVirtual().start(task));
            }

            for (var thread: threads) {
                thread.join();
            }

            LOGGER.info("evaluating {} expressions on {} profiles {} times took : {}s", expressions, records, evaluations, globalStopWatch.elapsed(TimeUnit.SECONDS));
        } catch (final Throwable throwable) {
            LOGGER.error("An error occurred while evaluating expression. See logs for more details.", throwable);
        }
    }

}