/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package voltdb.tasks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static voltdb.tasks.DrProducerLatency.MSG_ERROR_BACKLOG_POSITIVE;
import static voltdb.tasks.DrProducerLatency.MSG_ERROR_TSD_POSITIVE;
import static voltdb.tasks.DrProducerLatency.MSG_RATE_LIMIT_POSITIVE;
import static voltdb.tasks.DrProducerLatency.MSG_WARNING_BACKLOG_GT_ERROR;
import static voltdb.tasks.DrProducerLatency.MSG_WARNING_BACKLOG_POSITIVE;
import static voltdb.tasks.DrProducerLatency.MSG_WARNING_TSD_GT_ERROR;
import static voltdb.tasks.DrProducerLatency.MSG_WARNING_TSD_POSITIVE;

import java.sql.Timestamp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.task.Action;
import org.voltdb.task.ActionResult;
import org.voltdb.task.TaskHelper;

@ExtendWith(MockitoExtension.class)
public class DrProducerLatencyTest {
    @Mock
    private TaskHelper helper;

    @Mock
    private ClientResponse response;

    @Mock
    private ActionResult result;

    @Test
    public void validateParameters() {
        // Test valid parameters
        assertNull(DrProducerLatency.validateParameters(null, 500, 500, 1000, 1000, 5000));
        assertNull(DrProducerLatency.validateParameters(null, 0, 500, 1000, 1000, 5000));
        assertNull(DrProducerLatency.validateParameters(null, 500, 0, 1000, 1000, 5000));
        assertNull(DrProducerLatency.validateParameters(null, 500, 500, 0, 1000, 5000));
        assertNull(DrProducerLatency.validateParameters(null, 500, 500, 1000, 0, 5000));
        assertNull(DrProducerLatency.validateParameters(null, 500, 500, 1000, 1000, 0));

        // Test single invalid parameter
        assertEquals(DrProducerLatency.validateParameters(null, -1, 0, 0, 0, 0), MSG_WARNING_TSD_POSITIVE);
        assertEquals(DrProducerLatency.validateParameters(null, 0, -1, 0, 0, 0), MSG_WARNING_BACKLOG_POSITIVE);
        assertEquals(DrProducerLatency.validateParameters(null, 0, 0, -1, 0, 0), MSG_ERROR_TSD_POSITIVE);
        assertEquals(DrProducerLatency.validateParameters(null, 0, 0, 0, -1, 0), MSG_ERROR_BACKLOG_POSITIVE);
        assertEquals(DrProducerLatency.validateParameters(null, 200, 0, 100, 0, 0), MSG_WARNING_TSD_GT_ERROR);
        assertEquals(DrProducerLatency.validateParameters(null, 0, 200, 0, 100, 0), MSG_WARNING_BACKLOG_GT_ERROR);
        assertEquals(DrProducerLatency.validateParameters(null, 0, 0, 0, 0, -1), MSG_RATE_LIMIT_POSITIVE);

        // Test multiple invalid parameters
        assertErrorMessageContains(DrProducerLatency.validateParameters(null, -1, 0, -1, 0, 0),
                MSG_WARNING_TSD_POSITIVE, MSG_ERROR_TSD_POSITIVE);
        assertErrorMessageContains(DrProducerLatency.validateParameters(null, 0, -1, 0, -1, 0),
                MSG_WARNING_BACKLOG_POSITIVE, MSG_ERROR_BACKLOG_POSITIVE);
        assertErrorMessageContains(DrProducerLatency.validateParameters(null, -1, 0, 0, 0, -1),
                MSG_WARNING_TSD_POSITIVE, MSG_RATE_LIMIT_POSITIVE);
        assertErrorMessageContains(DrProducerLatency.validateParameters(null, 0, 0, -1, 0, -1), MSG_ERROR_TSD_POSITIVE,
                MSG_RATE_LIMIT_POSITIVE);
        assertErrorMessageContains(DrProducerLatency.validateParameters(null, 200, 0, 100, 0, -1),
                MSG_WARNING_TSD_GT_ERROR, MSG_RATE_LIMIT_POSITIVE);
    }

    /*
     * Test basic interaction between warning threshold, error threshold and rate limit
     */
    @Test
    public void basicLatencyLogging() {
        StatsRow[] rows = { new StatsRow(0), new StatsRow(1, 0) };
        Action action = initializeTask(100, 200, 1_000, rows);

        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // rows with no latency
        action.getCallback().apply(result);

        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // rows with some latency
        rows[0].latency(50);
        rows[1].latency(25);
        action.getCallback().apply(result);

        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // Add one row with warning latency
        rows[1].latency(120);
        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());
        verify(helper, never()).logError(any());

        // Same result should not log a second time
        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());
        verify(helper, never()).logError(any());

        // Same row but now as an error
        rows[1].latency(220);
        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());
        verify(helper, times(1)).logError(any());

        // Same result should not log a second time
        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());
        verify(helper, times(1)).logError(any());

        // Down grading latency to warning should not log
        rows[1].latency(120);
        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());
        verify(helper, times(1)).logError(any());

        // Other cluster going over threshold should warn
        rows[0].latency(120);
        rows[1].latency(0);
        action.getCallback().apply(result);

        verify(helper, times(2)).logWarning(any());
        verify(helper, times(1)).logError(any());
    }

    /*
     * Test that warnings are never logged when warning threshold is disabled
     */
    @Test
    public void disableWarning() {
        StatsRow[] rows = { new StatsRow(0), new StatsRow(1) };
        Action action = initializeTask(0, 200, 1_000, rows);

        // rows with no latency
        action.getCallback().apply(result);

        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // rows with some latency
        rows[0].latency(50);
        rows[1].latency(150);
        action.getCallback().apply(result);

        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // Add row with error latency
        rows[1].latency(250);
        action.getCallback().apply(result);

        verify(helper, never()).logWarning(any());
        verify(helper, times(1)).logError(any());
    }

    /*
     * Test that errors are never logged when error threshold is disabled
     */
    @Test
    public void disableErrors() {
        StatsRow[] rows = { new StatsRow(0), new StatsRow(1) };
        Action action = initializeTask(100, 0, 1_000, rows);

        // rows with no latency
        action.getCallback().apply(result);

        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // rows with warning latency
        rows[0].latency(50);
        rows[1].latency(150);
        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());
        verify(helper, never()).logError(any());

        // row with more latency
        rows[1].latency(250);
        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());
        verify(helper, never()).logError(any());

        // row with max latency
        rows[1].lastAckedTimestamp = new Timestamp(0);
        rows[1].lastQueuedTimestamp = new Timestamp(Long.MAX_VALUE);
        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());
        verify(helper, never()).logError(any());
    }

    /*
     * Test that rows that are not in normal mode or synced are ignored
     */
    @Test
    public void notRunningDrIsNotLogged() {
        StatsRow[] rows = { new StatsRow(0, 0).latency(2000).paused(), new StatsRow(1, 0).latency(2000).notSynced(),
                new StatsRow(2, 0).latency(120), new StatsRow(3, 0).latency(220) };
        Action action = initializeTask(100, 200, 1_000, rows);

        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());
        verify(helper, times(1)).logError(any());
    }

    /*
     * Test that all stats are ignored if result is not succes
     */
    @Test
    public void errorResponse() {
        Action action = initializeTask(100, 200, 1_000, null);

        when(response.getStatus()).thenReturn(ClientResponse.UNEXPECTED_FAILURE);
        action.getCallback().apply(result);

        verify(response, never()).getResults();
        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());
    }

    /*
     * Test that rate limit only applies to a cluster
     */
    @Test
    public void rateLimitMultiCluster() {
        StatsRow[] rows = { new StatsRow(0, 0), new StatsRow(0, 1), new StatsRow(1, 0), new StatsRow(1, 1) };

        Action action = initializeTask(100, 200, 1_000, rows);

        // rows with no latency
        action.getCallback().apply(result);
        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // cluster 0 with error latency
        rows[0].latency(220);
        rows[1].latency(220);
        action.getCallback().apply(result);
        verify(helper, never()).logWarning(any());
        verify(helper, times(1)).logError(any());

        // cluster 1 with warning latency
        rows[2].latency(120);
        rows[3].latency(120);
        action.getCallback().apply(result);
        verify(helper, times(1)).logWarning(any());
        verify(helper, times(1)).logError(any());
    }

    /*
     * Test no rate limit logs every time
     */
    @Test
    public void rateLimitDisabled() {
        StatsRow[] rows = { new StatsRow(0, 0).latency(120), new StatsRow(1, 0).latency(220) };
        Action action = initializeTask(100, 200, 0, rows);

        for (int i = 0; i < 10; ++i) {
            action.getCallback().apply(result);
            verify(helper, times(i + 1)).logWarning(any());
            verify(helper, times(i + 1)).logError(any());
        }
    }

    /*
     * Test that message will be logged once past ratelimit point
     */
    @Test
    public void logAfterRateLimit() throws InterruptedException {
        StatsRow[] rows = { new StatsRow(0).latency(120), new StatsRow(1).latency(220), new StatsRow(1, 0).latency(150),
                new StatsRow(1, 1) };
        Action action = initializeTask(100, 200, 20, rows);

        action.getCallback().apply(result);
        verify(helper, times(1)).logWarning(any());
        verify(helper, times(1)).logError(any());

        Thread.sleep(20);

        action.getCallback().apply(result);
        verify(helper, times(2)).logWarning(any());
        verify(helper, times(2)).logError(any());
    }

    /*
     * Test that over threshold messages are not logged when only one threshold is surpassed
     */
    @Test
    public void onlyLogWhenOverBothThresholds() {
        StatsRow[] rows = { new StatsRow(0).timestampDelta(150) };

        Action action = initializeTask(100, 200, 20, rows);

        // Only timestamp delta over warning
        action.getCallback().apply(result);
        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // Only timestamp delta over error
        rows[0].timestampDelta(250);
        action.getCallback().apply(result);
        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // Only backlog over warning
        rows[0].timestampDelta(0).backlog(150);
        action.getCallback().apply(result);
        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // Only backlog over error
        rows[0].backlog(250);
        action.getCallback().apply(result);
        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());
    }

    /*
     * Test that warning message is logged when one threshold is over error and the other is over warning
     */
    @Test
    public void statsAtDifferentThresholds() {
        StatsRow[] rows = { new StatsRow(0).timestampDelta(150).backlog(250) };

        Action action = initializeTask(100, 200, 0, rows);

        // delta over warning and backlog over error
        action.getCallback().apply(result);
        verify(helper, times(1)).logWarning(any());
        verify(helper, never()).logError(any());

        // delta over error and backlog over warning
        rows[0].timestampDelta(250).backlog(150);
        action.getCallback().apply(result);
        verify(helper, times(2)).logWarning(any());
        verify(helper, never()).logError(any());
    }

    private Action initializeTask(long warningTimestampDelta, long errorTimestampDelta, long rateLimit,
            StatsRow[] rows) {
        return initializeTask(warningTimestampDelta, warningTimestampDelta, errorTimestampDelta, errorTimestampDelta,
                rateLimit, rows);
    }

    private Action initializeTask(long warningTimestampDelta, long warningBacklog, long errorTimestampDelta,
            long errorBacklog, long rateLimit, StatsRow[] rows) {
        when(response.getStatus()).thenReturn(ClientResponse.SUCCESS);
        when(result.getResponse()).thenReturn(response);

        if (rows != null) {
            when(response.getResults()).then(c -> createTable(rows));
        }

        DrProducerLatency task = new DrProducerLatency();
        task.initialize(helper, warningTimestampDelta, warningBacklog, errorTimestampDelta, errorBacklog, rateLimit);
        return task.getFirstAction();
    }

    private static void assertErrorMessageContains(String errorMessage, String... expected) {
        assertNotNull(errorMessage);
        for (String e : expected) {
            assertTrue(errorMessage.contains(e), () -> String.format("'%s' does not contain '%s'", errorMessage, e));
        }
    }

    private static VoltTable[] createTable(StatsRow... rows) {
        VoltTable table = new VoltTable(new ColumnInfo("REMOTE_CLUSTER_ID", VoltType.INTEGER),
                new ColumnInfo("PARTITION_ID", VoltType.INTEGER),
                new ColumnInfo("LASTQUEUEDDRID", VoltType.BIGINT), new ColumnInfo("LASTACKDRID", VoltType.BIGINT),
                new ColumnInfo("LASTQUEUEDTIMESTAMP", VoltType.TIMESTAMP),
                new ColumnInfo("LASTACKTIMESTAMP", VoltType.TIMESTAMP), new ColumnInfo("ISSYNCED", VoltType.STRING),
                new ColumnInfo("MODE", VoltType.STRING));

        for (StatsRow row : rows) {
            table.addRow(row.toRow());
        }

        return new VoltTable[] { table };
    }

    static class StatsRow {
        final int remoteClusterId;
        final int partitionId;
        long lastQueuedDrid = 0;
        final long lastAckDrid = 0;
        Timestamp lastQueuedTimestamp = new Timestamp(System.currentTimeMillis());
        Timestamp lastAckedTimestamp = lastQueuedTimestamp;
        private boolean isSynced = true;
        private String mode = "NORMAL";

        StatsRow(int partitionId) {
            this(0, partitionId);
        }

        StatsRow(int remoteClusterId, int partitionId) {
            this.remoteClusterId = remoteClusterId;
            this.partitionId = partitionId;
        }

        StatsRow latency(long latency) {
            return timestampDelta(latency).backlog(latency);
        }

        StatsRow timestampDelta(long delta) {
            lastAckedTimestamp = new Timestamp(lastQueuedTimestamp.getTime() - delta);
            return this;
        }

        StatsRow backlog(long backlog) {
            lastQueuedDrid = backlog;
            return this;
        }

        StatsRow notSynced() {
            isSynced = false;
            return this;
        }

        StatsRow paused() {
            mode = "PAUSED";
            return this;
        }

        Object[] toRow() {
            return new Object[] { remoteClusterId, partitionId, lastQueuedDrid, lastAckDrid, lastQueuedTimestamp,
                    lastAckedTimestamp, Boolean.toString(isSynced), mode };
        }
    }
}
