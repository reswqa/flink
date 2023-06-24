package org.apache.flink.processfunction;

import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.functions.SingleStreamFilterFunction;
import org.apache.flink.processfunction.functions.SingleStreamMapFunction;
import org.apache.flink.processfunction.functions.SingleStreamReduceFunction;

public class FunctionImpl {
    public static <IN, OUT> SingleStreamProcessFunction<IN, OUT> map(
            BatchStreamingUnifiedFunctions.MapFunction<IN, OUT> mapFunc) {
        return new SingleStreamMapFunction<>(mapFunc);
    }

    public static <T> SingleStreamProcessFunction<T, T> filter(
            BatchStreamingUnifiedFunctions.FilterFunction<T> filterFunc) {
        return new SingleStreamFilterFunction<>(filterFunc);
    }

    public static <T> SingleStreamProcessFunction<T, T> reduce(
            BatchStreamingUnifiedFunctions.ReduceFunction<T> reduceFunc) {
        return new SingleStreamReduceFunction<>(reduceFunc);
    }
}
