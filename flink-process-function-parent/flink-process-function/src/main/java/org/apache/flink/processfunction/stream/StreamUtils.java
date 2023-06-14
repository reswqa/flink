package org.apache.flink.processfunction.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.processfunction.api.function.Functions;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.functions.SingleStreamFilterFunction;
import org.apache.flink.processfunction.functions.SingleStreamMapFunction;
import org.apache.flink.processfunction.functions.SingleStreamReduceFunction;

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
}
