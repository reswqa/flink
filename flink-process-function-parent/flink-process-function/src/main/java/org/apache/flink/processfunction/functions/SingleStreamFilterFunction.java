package org.apache.flink.processfunction.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;

import java.util.function.Consumer;

/** The built-in {@link SingleStreamProcessFunction} implementation for {@link FilterFunction}. */
public class SingleStreamFilterFunction<IN> implements SingleStreamProcessFunction<IN, IN> {

    private final BatchStreamingUnifiedFunctions.FilterFunction<IN> filterFunction;

    public SingleStreamFilterFunction(
            BatchStreamingUnifiedFunctions.FilterFunction<IN> filterFunction) {
        this.filterFunction = filterFunction;
    }

    @Override
    public void processRecord(IN record, Consumer<IN> output, RuntimeContext ctx) throws Exception {
        if (filterFunction.filter(record)) {
            output.accept(record);
        }
    }
}
