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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static voltdb.tasks.GrowingCommandlog.MSG_ERROR_BAD_SEGMENT_CNT;

import java.util.function.Function;

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
public class GrowingCommandlogTest {
    @Mock
    private TaskHelper helper;

    @Mock
    private ClientResponse response;

    @Mock
    private ActionResult result;

    @Test
    public void validateParameters() {
        // Test valid parameters
        assertNull(GrowingCommandlog.validateParameters(null, 1));
        assertNull(GrowingCommandlog.validateParameters(null, 2));

        // Test single invalid parameter
        assertEquals(MSG_ERROR_BAD_SEGMENT_CNT, GrowingCommandlog.validateParameters(null, 0));
    }

    /*
     * Test warning threshold
     */
    @Test
    public void basicLogging() {
        StatsRow[] rows = { new StatsRow("A", 1), new StatsRow("B", 1), new StatsRow("C", 1) };
        Action action = initializeTask(10, rows);

        verify(helper, never()).logWarning(any());
        verify(helper, never()).logError(any());

        // rows with 1 in-use segment
        Function<ActionResult, Action> rslt = action.getCallback();
        rslt.apply(result);

        verify(helper, never()).logWarning(any());

        // 1 row with a segment count equal to threshold (should not trigger log message)
        rows[0].inUse(1);
        rows[1].inUse(2);
        rows[2].inUse(10);
        action.getCallback().apply(result);

        verify(helper, never()).logWarning(any());

        // Add one row a segment count above the threshold
        rows[1].inUse(12);
        action.getCallback().apply(result);

        verify(helper, times(1)).logWarning(any());

        // A second row has a segment count above the threshold
        rows[2].inUse(22);
        action.getCallback().apply(result);

        verify(helper, times(2)).logWarning(any());
    }

    /*
     * Test that warning message is logged with 2 hostnames when both are over the threshold
     */
    @Test
    public void correctWarningString() {
        StatsRow[] rows = { new StatsRow("A", 1), new StatsRow("B", 1), new StatsRow("C", 1) };

        Action action = initializeTask(10, rows);

        // no warning yet
        action.getCallback().apply(result);
        verify(helper, never()).logWarning(any());

        // 2 hosts over the threshold
        rows[0].inUse(15);
        rows[2].inUse(11);
        action.getCallback().apply(result);
        verify(helper, times(1)).logWarning(contains("A, C"));
        verify(helper, times(1)).logWarning(contains("between 11 and 15"));
    }


    private Action initializeTask(int maxSegmentsInUse, StatsRow[] rows) {
        when(response.getStatus()).thenReturn(ClientResponse.SUCCESS);
        when(result.getResponse()).thenReturn(response);

        if (rows != null) {
            when(response.getResults()).then(c -> createTable(rows));
        }

        GrowingCommandlog task = new GrowingCommandlog();
        task.initialize(helper, maxSegmentsInUse);
        return task.getFirstAction();
    }

    private static VoltTable[] createTable(StatsRow... rows) {
        VoltTable table = new VoltTable(
                new ColumnInfo("HOSTNAME", VoltType.STRING),
                new ColumnInfo("IN_USE_SEGMENT_COUNT", VoltType.INTEGER));

        for (StatsRow row : rows) {
            table.addRow(row.toRow());
        }

        return new VoltTable[] { table };
    }

    static class StatsRow {
        final String hostname;
        int inuseSegments;

        StatsRow(String hostname, int inuseSegments) {
            this.hostname = hostname;
            this.inuseSegments = inuseSegments;
        }

        StatsRow inUse(int inuseSegments) {
            this.inuseSegments = inuseSegments;
            return this;
        }

        Object[] toRow() {
            return new Object[] { hostname, inuseSegments };
        }
    }
}
