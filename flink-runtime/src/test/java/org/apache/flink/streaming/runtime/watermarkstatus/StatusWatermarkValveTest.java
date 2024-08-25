/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.GeneralizedWatermarkElement;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link StatusWatermarkValve}. While tests in {@link
 * org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTest} and {@link
 * org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTest} may also implicitly test {@link
 * StatusWatermarkValve} and that valves are correctly used in the tasks' input processors, the unit
 * tests here additionally makes sure that the watermarks and watermark statuses to forward are
 * generated from the valve at the exact correct times and in a deterministic behaviour. The unit
 * tests here also test more complex watermark status / watermark input cases.
 *
 * <p>The tests are performed by a series of watermark and watermark status inputs to the valve. On
 * every input method call, the output is checked to contain only the expected watermark or stream
 * status, and nothing else. This ensures that no redundant outputs are generated by the output
 * logic of {@link StatusWatermarkValve}. The behaviours that a series of input calls to the valve
 * is trying to test is explained as inline comments within the tests.
 */
class StatusWatermarkValveTest {

    /**
     * Tests that watermarks correctly advance with increasing watermarks for a single input valve.
     */
    @Test
    void testSingleInputIncreasingWatermarks() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(1);

        valve.inputWatermark(new Watermark(0), 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(0));
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(25));
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /**
     * Tests that watermarks do not advance with decreasing watermark inputs for a single input
     * valve.
     */
    @Test
    void testSingleInputDecreasingWatermarksYieldsNoOutput() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(1);

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(25));

        valve.inputWatermark(new Watermark(18), 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermark(new Watermark(42), 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(42));
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /**
     * Tests that watermark status toggling works correctly, as well as that non-toggling status
     * inputs do not yield output for a single input valve.
     */
    @Test
    void testSingleInputWatermarkStatusToggling() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(1);

        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 0, valveOutput);
        // this also implicitly verifies that input channels start as ACTIVE
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(WatermarkStatus.IDLE);

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(WatermarkStatus.ACTIVE);
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /** Tests that the watermark of an input channel remains intact while in the IDLE status. */
    @Test
    void testSingleInputWatermarksIntactDuringIdleness() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(1);

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(25));
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(WatermarkStatus.IDLE);

        valve.inputWatermark(new Watermark(50), 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();
        assertThat(valve.getSubpartitionStatus(0).watermark).isEqualTo(25);

        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(WatermarkStatus.ACTIVE);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermark(new Watermark(50), 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(50));
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /** Tests that the valve yields a watermark only when all inputs have received a watermark. */
    @Test
    void testMultipleInputYieldsWatermarkOnlyWhenAllChannelsReceivesWatermarks() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(0), 0, valveOutput);
        valve.inputWatermark(new Watermark(0), 1, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        // now, all channels have watermarks
        valve.inputWatermark(new Watermark(0), 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(0));
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /**
     * Tests that new min watermark is emitted from the valve as soon as the overall new min
     * watermark across inputs advances.
     */
    @Test
    void testMultipleInputIncreasingWatermarks() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(0), 0, valveOutput);
        valve.inputWatermark(new Watermark(0), 1, valveOutput);
        valve.inputWatermark(new Watermark(0), 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(0));

        valve.inputWatermark(new Watermark(12), 0, valveOutput);
        valve.inputWatermark(new Watermark(8), 2, valveOutput);
        valve.inputWatermark(new Watermark(10), 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermark(new Watermark(15), 1, valveOutput);
        // lowest watermark across all channels is now channel 2, with watermark @ 10
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(10));
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermark(new Watermark(17), 2, valveOutput);
        // lowest watermark across all channels is now channel 0, with watermark @ 12
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(12));
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermark(new Watermark(20), 0, valveOutput);
        // lowest watermark across all channels is now channel 1, with watermark @ 15
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(15));
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /** Tests that for a multiple input valve, decreasing watermarks will yield no output. */
    @Test
    void testMultipleInputDecreasingWatermarksYieldsNoOutput() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        valve.inputWatermark(new Watermark(10), 1, valveOutput);
        valve.inputWatermark(new Watermark(17), 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(10));

        valve.inputWatermark(new Watermark(12), 0, valveOutput);
        valve.inputWatermark(new Watermark(8), 1, valveOutput);
        valve.inputWatermark(new Watermark(15), 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /**
     * Tests that watermark status toggling works correctly, as well as that non-toggling status
     * inputs do not yield output for a multiple input valve.
     */
    @Test
    void testMultipleInputWatermarkStatusToggling() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(2);

        // this also implicitly verifies that all input channels start as active
        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 0, valveOutput);
        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 1, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 1, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        // now, all channels are IDLE
        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(WatermarkStatus.IDLE);

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 0, valveOutput);
        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 1, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        // as soon as at least one input becomes active again, the ACTIVE marker should be forwarded
        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 1, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(WatermarkStatus.ACTIVE);

        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 0, valveOutput);
        // already back to ACTIVE, should yield no output
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /**
     * Tests that for multiple inputs, when some inputs are idle, the min watermark is correctly
     * computed and advanced from the remaining active inputs.
     */
    @Test
    void testMultipleInputWatermarkAdvancingWithPartiallyIdleChannels() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(15), 0, valveOutput);
        valve.inputWatermark(new Watermark(10), 1, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 2, valveOutput);
        // min watermark should be computed from remaining ACTIVE channels
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(10));
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermark(new Watermark(18), 1, valveOutput);
        // now, min watermark should be 15 from channel #0
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(15));
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermark(new Watermark(20), 0, valveOutput);
        // now, min watermark should be 18 from channel #1
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(18));
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /**
     * Tests that as input channels individually and gradually become idle, watermarks are output as
     * soon remaining active channels can yield a new min watermark.
     */
    @Test
    void testMultipleInputWatermarkAdvancingAsChannelsIndividuallyBecomeIdle() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(25), 0, valveOutput);
        valve.inputWatermark(new Watermark(10), 1, valveOutput);
        valve.inputWatermark(new Watermark(17), 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(10));

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 1, valveOutput);
        // only channel 0 & 2 is ACTIVE; 17 is the overall min watermark now
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(17));

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 2, valveOutput);
        // only channel 0 is ACTIVE; 25 is the overall min watermark now
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(25));
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /**
     * Tests that when all inputs become idle, the max watermark across all channels is correctly
     * "flushed" from the valve, as well as the watermark status IDLE marker.
     *
     * <p>This test along with {@link
     * #testMultipleInputWatermarkAdvancingAsChannelsIndividuallyBecomeIdle} should completely
     * verify that the eventual watermark advancement result when all inputs become idle is
     * independent of the order that the inputs become idle.
     */
    @Test
    void testMultipleInputFlushMaxWatermarkAndWatermarkStatusOnceAllInputsBecomeIdle()
            throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        // -------------------------------------------------------------------------------------------
        // Setup valve for test case:
        //  channel #1: Watermark 10, ACTIVE
        //  channel #2: Watermark 5, ACTIVE
        //  channel #3: Watermark 3, ACTIVE
        //  Min Watermark across channels = 3 (from channel #3)
        // -------------------------------------------------------------------------------------------

        valve.inputWatermark(new Watermark(10), 0, valveOutput);
        valve.inputWatermark(new Watermark(5), 1, valveOutput);
        valve.inputWatermark(new Watermark(3), 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(3));

        // -------------------------------------------------------------------------------------------
        // Order of becoming IDLE:
        //  channel #1 ----------------> channel #2 ----------------> channel #3
        //   |-> (nothing emitted)        |-> (nothing emitted)        |-> Emit Watermark(10) & IDLE
        // -------------------------------------------------------------------------------------------

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 0, valveOutput);
        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 1, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(10));
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(WatermarkStatus.IDLE);
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /**
     * Tests that when idle channels become active again, they need to "catch up" with the latest
     * watermark before they are considered for min watermark computation again.
     */
    @Test
    void testMultipleInputWatermarkRealignmentAfterResumeActive() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(10), 0, valveOutput);
        valve.inputWatermark(new Watermark(7), 1, valveOutput);
        valve.inputWatermark(new Watermark(3), 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(3));
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(7));
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        // let channel 2 become active again; since the min watermark has now advanced to 7,
        // channel 2 should have been marked as non-aligned.
        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 2, valveOutput);
        assertThat(valve.getSubpartitionStatus(2).isWatermarkAligned).isFalse();

        // during the realignment process, watermarks should still be accepted by channel 2 (but
        // shouldn't yield new watermarks)
        valve.inputWatermark(new Watermark(5), 2, valveOutput);
        assertThat(valve.getSubpartitionStatus(2).watermark).isEqualTo(5);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        // let channel 2 catch up with the min watermark; now should be realigned
        valve.inputWatermark(new Watermark(9), 2, valveOutput);
        assertThat(valve.getSubpartitionStatus(2).isWatermarkAligned).isTrue();
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        // check that realigned inputs is now taken into account for watermark advancement
        valve.inputWatermark(new Watermark(12), 1, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(9));
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    /**
     * Verify that we don't see any state changes/watermarks when all ACTIVE channels are unaligned.
     * Earlier versions of the valve had a bug that would cause it to emit a {@code Long.MAX_VALUE}
     * watermark in that case.
     */
    @Test
    void testNoOutputWhenAllActiveChannelsAreUnaligned() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(3);

        valve.inputWatermark(new Watermark(10), 0, valveOutput);
        valve.inputWatermark(new Watermark(7), 1, valveOutput);

        // make channel 2 ACTIVE, it is now in "catch up" mode (unaligned watermark)
        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(7));
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        // make channel 2 ACTIVE again, it is still unaligned
        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 2, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        // make channel 0 and 1 IDLE, now channel 2 is the only ACTIVE channel but it's unaligned
        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 0, valveOutput);
        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 1, valveOutput);

        // we should not see any output
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    @Test
    void testUnalignedActiveChannelBecomesIdle() throws Exception {
        StatusWatermarkOutput valveOutput = new StatusWatermarkOutput();
        StatusWatermarkValve valve = new StatusWatermarkValve(2);

        valve.inputWatermark(new Watermark(7), 0, valveOutput);
        valve.inputWatermark(new Watermark(10), 1, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(7));

        // make channel 0 idle, it becomes unaligned
        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isEqualTo(new Watermark(10));

        // make channel 0 active, but it is still unaligned because it's watermark < last output
        // watermark
        valve.inputWatermarkStatus(WatermarkStatus.ACTIVE, 0, valveOutput);
        valve.inputWatermark(new Watermark(9), 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();

        // make channel 0 idle again
        valve.inputWatermarkStatus(WatermarkStatus.IDLE, 0, valveOutput);
        assertThat(valveOutput.popLastSeenOutput()).isNull();
    }

    private static class StatusWatermarkOutput implements PushingAsyncDataInput.DataOutput {

        private BlockingQueue<StreamElement> allOutputs = new LinkedBlockingQueue<>();

        @Override
        public void emitWatermark(Watermark watermark) {
            allOutputs.add(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            allOutputs.add(watermarkStatus);
        }

        @Override
        public void emitRecord(StreamRecord record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitGeneralizedWatermark(GeneralizedWatermarkElement watermark)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        public StreamElement popLastSeenOutput() {
            return allOutputs.poll();
        }
    }
}
