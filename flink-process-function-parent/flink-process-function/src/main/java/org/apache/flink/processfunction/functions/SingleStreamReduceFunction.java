package org.apache.flink.processfunction.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;

/** The built-in {@link SingleStreamProcessFunction} implementation for {@link ReduceFunction}. */
public class SingleStreamReduceFunction<IN> implements SingleStreamProcessFunction<IN, IN> {
    private final org.apache.flink.processfunction.api.function.ReduceFunction<IN> reduceFunction;

    public SingleStreamReduceFunction(
            org.apache.flink.processfunction.api.function.ReduceFunction<IN> reduceFunc) {
        this.reduceFunction = reduceFunc;
    }

    @Override
    public void processRecord(IN record, Collector<IN> output, RuntimeContext ctx) {
        // Do nothing as this will translator to reduceOperator instead of processOperator.
    }

    public org.apache.flink.processfunction.api.function.ReduceFunction<IN> getReduceFunction() {
        return reduceFunction;
    }
}
