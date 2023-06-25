package org.apache.flink.processfunction.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.processfunction.DataStream;
import org.apache.flink.processfunction.api.builtin.BatchStreamingUnifiedFunctions;
import org.apache.flink.processfunction.api.function.SingleStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoInputStreamProcessFunction;
import org.apache.flink.processfunction.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.processfunction.connector.ConsumerSinkFunction;
import org.apache.flink.processfunction.functions.SingleStreamFilterFunction;
import org.apache.flink.processfunction.functions.SingleStreamMapFunction;
import org.apache.flink.processfunction.functions.SingleStreamReduceFunction;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
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
                            BatchStreamingUnifiedFunctions.MapFunction.class,
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

    public static <IN1, IN2, OUT> TypeInformation<OUT> getOutputTypeForTwoInputProcessFunction(
            TwoInputStreamProcessFunction<IN1, IN2, OUT> processFunction,
            TypeInformation<IN1> in1TypeInformation,
            TypeInformation<IN2> in2TypeInformation) {
        return TypeExtractor.getBinaryOperatorReturnType(
                processFunction,
                TwoInputStreamProcessFunction.class,
                0,
                1,
                2,
                TypeExtractor.NO_INDEX,
                in1TypeInformation,
                in2TypeInformation,
                Utils.getCallLocationName(),
                true);
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

    public static <IN1, IN2, OUT> Transformation<OUT> getTwoInputTransform(
            String functionName,
            DataStream<IN1> inputStream1,
            DataStream<IN2> inputStream2,
            TypeInformation<OUT> outTypeInformation,
            TwoInputStreamOperator<IN1, IN2, OUT> operator) {
        TwoInputTransformation<IN1, IN2, OUT> transform =
                new TwoInputTransformation<>(
                        inputStream1.getTransformation(),
                        inputStream2.getTransformation(),
                        functionName,
                        SimpleOperatorFactory.of(operator),
                        outTypeInformation,
                        // TODO Supports configure parallelism
                        1,
                        true);

        TypeInformation<?> keyType = null;
        if (inputStream1 instanceof KeyedPartitionStreamImpl) {
            KeyedPartitionStreamImpl<?, IN1> keyedInput1 =
                    (KeyedPartitionStreamImpl<?, IN1>) inputStream1;

            keyType = keyedInput1.getKeyType();

            transform.setStateKeySelectors(keyedInput1.getKeySelector(), null);
            transform.setStateKeyType(keyType);
        }
        if (inputStream2 instanceof KeyedPartitionStreamImpl) {
            KeyedPartitionStreamImpl<?, IN2> keyedInput2 =
                    (KeyedPartitionStreamImpl<?, IN2>) inputStream2;

            TypeInformation<?> keyType2 = keyedInput2.getKeyType();

            if (keyType != null && !(keyType.canEqual(keyType2) && keyType.equals(keyType2))) {
                throw new UnsupportedOperationException(
                        "Key types if input KeyedStreams "
                                + "don't match: "
                                + keyType
                                + " and "
                                + keyType2
                                + ".");
            }

            transform.setStateKeySelectors(
                    transform.getStateKeySelector1(), keyedInput2.getKeySelector());

            // we might be overwriting the one that's already set, but it's the same
            transform.setStateKeyType(keyType2);
        }

        return transform;
    }

    public static <IN, OUT1, OUT2>
            Tuple2<TypeInformation<OUT1>, TypeInformation<OUT2>> getTwoOutputType(
                    TwoOutputStreamProcessFunction<IN, OUT1, OUT2> twoOutputStreamProcessFunction,
                    TypeInformation<IN> inTypeInformation) {
        TypeInformation<OUT1> firstOutputType =
                TypeExtractor.getUnaryOperatorReturnType(
                        twoOutputStreamProcessFunction,
                        TwoOutputStreamProcessFunction.class,
                        0,
                        1,
                        new int[] {1, 0},
                        inTypeInformation,
                        Utils.getCallLocationName(),
                        true);
        TypeInformation<OUT2> secondOutputType =
                TypeExtractor.getUnaryOperatorReturnType(
                        twoOutputStreamProcessFunction,
                        TwoOutputStreamProcessFunction.class,
                        0,
                        2,
                        new int[] {2, 0},
                        inTypeInformation,
                        Utils.getCallLocationName(),
                        true);
        return Tuple2.of(firstOutputType, secondOutputType);
    }
}
