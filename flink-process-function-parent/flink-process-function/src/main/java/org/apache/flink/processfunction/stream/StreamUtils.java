package org.apache.flink.processfunction.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.processfunction.api.function.Functions;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.connector.ConsumerSinkFunction;
import org.apache.flink.processfunction.functions.SingleStreamFilterFunction;
import org.apache.flink.processfunction.functions.SingleStreamMapFunction;
import org.apache.flink.processfunction.functions.SingleStreamReduceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.util.function.ConsumerFunction;

/** Utils for all streams. */
public class StreamUtils {
    public static <IN, OUT> TypeInformation<OUT> getOutputTypeForProcessFunction(
            SingleStreamProcessFunction<IN, OUT> processFunction,
            TypeInformation<IN> inTypeInformation) {
        TypeInformation<OUT> outType;
        if (processFunction instanceof SingleStreamMapFunction) {
            outType =
                    TypeExtractor.getUnaryOperatorReturnType(
                            ((SingleStreamMapFunction<IN, OUT>) processFunction).getMapFunction(),
                            Functions.MapFunction.class,
                            0,
                            1,
                            TypeExtractor.NO_INDEX,
                            inTypeInformation,
                            Utils.getCallLocationName(),
                            true);
        } else if (processFunction instanceof SingleStreamFilterFunction
                || processFunction instanceof SingleStreamReduceFunction) {
            //noinspection unchecked
            outType = (TypeInformation<OUT>) inTypeInformation;
        } else {
            outType =
                    TypeExtractor.getUnaryOperatorReturnType(
                            processFunction,
                            SingleStreamProcessFunction.class,
                            0,
                            1,
                            new int[] {1, 0},
                            inTypeInformation,
                            Utils.getCallLocationName(),
                            true);
        }
        return outType;
    }

    public static <T> Transformation<T> getConsumerSinkTransform(
            Transformation<T> inputTransform, ConsumerFunction<T> consumer) {
        // read the output type of the input Transform to coax out errors about MissingTypeInfo
        inputTransform.getOutputType();

        ConsumerSinkFunction<T> sinkFunction = new ConsumerSinkFunction<>(consumer);

        // TODO Supports clean closure
        StreamSink<T> sinkOperator = new StreamSink<>(sinkFunction);

        return new LegacySinkTransformation<>(
                inputTransform,
                "Consumer Sink",
                sinkOperator,
                // TODO Supports configure parallelism
                1,
                true);
    }
}
