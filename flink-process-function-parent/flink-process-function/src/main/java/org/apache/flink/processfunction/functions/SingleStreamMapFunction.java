package org.apache.flink.processfunction.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.processfunction.api.Collector;
import org.apache.flink.processfunction.api.RuntimeContext;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;

/** The built-in {@link SingleStreamProcessFunction} implementation for {@link MapFunction}. */
public class SingleStreamMapFunction<IN, OUT> implements SingleStreamProcessFunction<IN, OUT> {

    private final BatchStreamingUnifiedFunctions.MapFunction<IN, OUT> mapFunction;

    public SingleStreamMapFunction(
            BatchStreamingUnifiedFunctions.MapFunction<IN, OUT> mapFunction) {
        this.mapFunction = mapFunction;
    }

    public BatchStreamingUnifiedFunctions.MapFunction<IN, OUT> getMapFunction() {
        return mapFunction;
    }

    @Override
    public void processRecord(IN record, Collector<OUT> output, RuntimeContext ctx)
            throws Exception {
        output.collect(mapFunction.map(record));
    }
}
