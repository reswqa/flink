package org.apache.flink.datastream.impl.extension.window.utils;

import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.datastream.api.extension.window.BaseWindowProcessFunction;
import org.apache.flink.datastream.api.extension.window.window.Window;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperator;
import org.apache.flink.runtime.state.v2.ListStateDescriptor;
import org.apache.flink.runtime.state.v2.MapStateDescriptor;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.runtime.state.v2.adaptor.ListStateAdaptor;
import org.apache.flink.runtime.state.v2.adaptor.MapStateAdaptor;
import org.apache.flink.runtime.state.v2.adaptor.ValueStateAdaptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class WindowStateStore<K, W extends Window> {

    protected static final Logger LOG = LoggerFactory.getLogger(WindowStateStore.class);

    private BaseWindowProcessFunction<W> windowProcessFunction;

    private AbstractAsyncStateStreamOperator<?> operator;

    private KeyedStateProvider keyedStateProvider;

    protected TypeSerializer<W> windowSerializer;

    private boolean isStateDeclared(StateDeclaration stateDeclaration) {
        if (!windowProcessFunction.useWindowStates().contains(stateDeclaration)) {
            LOG.warn(
                    "Fail to get window state for "
                            + stateDeclaration.getName()
                            + ", please declare the used state in the `useWindowStates` method first.");
            return false;
        }
        return true;
    }

    private boolean stateRedistributionModeIsNone(StateDeclaration stateDeclaration) {
        StateDeclaration.RedistributionMode redistributionMode =
                stateDeclaration.getRedistributionMode();
        return redistributionMode == StateDeclaration.RedistributionMode.NONE;
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<ListState<T>> getWindowState(
            ListStateDeclaration<T> stateDeclaration, W namespace) {
        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        ListStateDescriptor<T> stateDescriptor =
                new ListStateDescriptor<T>(
                        stateDeclaration.getName(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getTypeDescriptor().getTypeClass()));

        try {
            ListStateAdaptor<K, W, T> state =
                    (ListStateAdaptor<K, W, T>)
                            keyedStateProvider.getOrCreateKeyedState(
                                    namespace, windowSerializer, stateDescriptor);
            state.setCurrentNamespace(namespace);
            return Optional.of(state);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public <KEY, V> Optional<MapState<KEY, V>> getWindowState(
            MapStateDeclaration<KEY, V> stateDeclaration, W namespace) {
        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        MapStateDescriptor<KEY, V> stateDescriptor =
                new MapStateDescriptor<KEY, V>(
                        stateDeclaration.getName(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getKeyTypeDescriptor().getTypeClass()),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getValueTypeDescriptor().getTypeClass()));

        try {
            MapStateAdaptor<K, W, KEY, V> state =
                    (MapStateAdaptor<K, W, KEY, V>)
                            keyedStateProvider.getOrCreateKeyedState(
                                    namespace, windowSerializer, stateDescriptor);
            state.setCurrentNamespace(namespace);
            return Optional.of(state);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public <T> Optional<ValueState<T>> getWindowState(
            ValueStateDeclaration<T> stateDeclaration, W namespace) {
        if (!isStateDeclared(stateDeclaration)) {
            return Optional.empty();
        }

        if (stateRedistributionModeIsNone(stateDeclaration)) {
            throw new UnsupportedOperationException(
                    "RedistributionMode "
                            + stateDeclaration.getRedistributionMode().name()
                            + " is not supported for window state.");
        }

        ValueStateDescriptor<T> stateDescriptor =
                new ValueStateDescriptor<T>(
                        stateDeclaration.getName(),
                        TypeExtractor.createTypeInfo(
                                stateDeclaration.getTypeDescriptor().getTypeClass()));

        try {
            ValueStateAdaptor<K, W, T> state =
                    (ValueStateAdaptor<K, W, T>)
                            keyedStateProvider.getOrCreateKeyedState(
                                    namespace, windowSerializer, stateDescriptor);
            state.setCurrentNamespace(namespace);
            return Optional.of(state);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @FunctionalInterface
    public interface KeyedStateProvider<N, S extends State, T> {
        S getOrCreateKeyedState(
                N defaultNamespace,
                TypeSerializer<N> namespaceSerializer,
                StateDescriptor<T> stateDescriptor)
                throws Exception;
    }
}
