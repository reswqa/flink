package org.apache.flink.processfunction.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;

import java.util.function.Consumer;

/** The built-in {@link SingleStreamProcessFunction} implementation for {@link ReduceFunction}. */
public class SingleStreamReduceFunction<IN> implements SingleStreamProcessFunction<IN, IN> {
    private final BatchStreamingUnifiedFunctions.ReduceFunction<IN> reduceFunction;

    public SingleStreamReduceFunction(
            BatchStreamingUnifiedFunctions.ReduceFunction<IN> reduceFunc) {
        this.reduceFunction = reduceFunc;
    }

    @Override
    public void processRecord(IN record, Consumer<IN> output, RuntimeContext ctx) {
        // Do nothing as this will translator to reduceOperator instead of processOperator.
    }

    public BatchStreamingUnifiedFunctions.ReduceFunction<IN> getReduceFunction() {
        return reduceFunction;
    }
}
