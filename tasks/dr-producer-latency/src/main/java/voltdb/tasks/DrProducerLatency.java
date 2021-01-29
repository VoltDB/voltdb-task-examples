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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.task.Action;
import org.voltdb.task.ActionGenerator;
import org.voltdb.task.ActionResult;
import org.voltdb.task.TaskHelper;
import org.voltdb.utils.CompoundErrors;

/**
 * Task for polling the DRPRODUCER statistics and reporting when latency is higher than a given threshold.
 * <p>
 * Example task creation:<br>
 *
 * <pre>
 * CREATE TASK dr_producer_latency ON SCHEDULE EVERY 5 MINUTES
 *     PROCEDURE FROM CLASS voltdb.tasks.DrProducerLatency WITH (500, 1000, 900000)
 *     ON ERROR LOG;
 * </pre>
 * <p>
 * Parameters which need to be provided when the task is defined
 * <table summary="Task Parameters">
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>Warning time stamp delta</td>
 * <td>The time stamp delta threshold, in milliseconds, at which point a warning message will be logged if warning
 * backlog is also exceeded. If 0 threshold is disabled. The delta is calculated by subtracting LASTACKTIMESTAMP from
 * LASTQUEUEDTIMESTAMP</td>
 * </tr>
 * <tr>
 * <td>Warning backlog</td>
 * <td>The backlog threshold for number of outstanding transactions to replicate. If the threshold is exceeded and the
 * warning time stamp delta is exceeded a warning message will be logged. If 0 threshold is disabled. The backlog is
 * calculated by subtracting LASTACKDRID from LASTQUEUEDDRID</td>
 * </tr>
 * <tr>
 * <td>Error time stamp delta</td>
 * <td>The time stamp delta threshold, in milliseconds, at which point an error message will be logged if error backlog
 * is also exceeded. If 0 threshold is disabled. The delta is calculated by subtracting LASTACKTIMESTAMP from
 * LASTQUEUEDTIMESTAMP</td>
 * </tr>
 * <tr>
 * <td>Error backlog</td>
 * <td>The backlog threshold for number of outstanding transactions to replicate. If the threshold is exceeded and the
 * error time stamp delta is exceeded an error message will be logged. If 0 threshold is disabled. The backlog is
 * calculated by subtracting LASTACKDRID from LASTQUEUEDDRID</td>
 * </tr>
 * <tr>
 * <td>Rate limit</td>
 * <td>Rate limit, in milliseconds, for log messages per remote cluster and log level</td>
 * </tr>
 * </table>
 */
public class DrProducerLatency implements ActionGenerator {
    // Messages visible for test validation
    final static String MSG_WARNING_TSD_POSITIVE = "Warning time stamp delta must be >= 0.";
    final static String MSG_ERROR_TSD_POSITIVE = "Error time stamp delta be >= 0.";
    final static String MSG_WARNING_TSD_GT_ERROR = "Warning time stamp delta must be <= error time stamp delta.";
    final static String MSG_WARNING_BACKLOG_POSITIVE = "Warning backlog must be >= 0.";
    final static String MSG_ERROR_BACKLOG_POSITIVE = "Error backlog must be >= 0.";
    final static String MSG_WARNING_BACKLOG_GT_ERROR = "Warning backlog must be <= error backlog.";
    final static String MSG_RATE_LIMIT_POSITIVE = "Rate limit must be >= 0.";

    private final Map<Integer, LastReported> previouslyReported = new LinkedHashMap<>();

    private TaskHelper helper;
    private long warningTimestampDelta;
    private long warningBacklog;
    private long errorTimestampDelta;
    private long errorBacklog;
    private long rateLimitNs;

    public static String validateParameters(TaskHelper helper, long warningTimestampDelta, long warningBacklog,
            long errorTimestampDelta, long errorBacklog, long rateLimitMs) {
        CompoundErrors errors = new CompoundErrors();
        if (warningTimestampDelta < 0) {
            errors.addErrorMessage(MSG_WARNING_TSD_POSITIVE);
        }
        if (warningBacklog < 0) {
            errors.addErrorMessage(MSG_WARNING_BACKLOG_POSITIVE);
        }
        if (errorTimestampDelta < 0) {
            errors.addErrorMessage(MSG_ERROR_TSD_POSITIVE);
        }
        if (errorBacklog < 0) {
            errors.addErrorMessage(MSG_ERROR_BACKLOG_POSITIVE);
        }
        if (warningTimestampDelta > errorTimestampDelta && errorTimestampDelta > 0 && warningTimestampDelta > 0) {
            errors.addErrorMessage(MSG_WARNING_TSD_GT_ERROR);
        }
        if (warningBacklog > errorBacklog && errorBacklog > 0 && warningBacklog > 0) {
            errors.addErrorMessage(MSG_WARNING_BACKLOG_GT_ERROR);
        }
        if (rateLimitMs < 0) {
            errors.addErrorMessage(MSG_RATE_LIMIT_POSITIVE);
        }
        return errors.getErrorMessage();
    }

    public void initialize(TaskHelper helper, long warningTimestampDelta, long warningBacklog, long errorTimestampDelta,
            long errorBacklog, long rateLimitMs) {
        this.helper = helper;
        if (warningTimestampDelta == 0 || warningBacklog == 0) {
            this.warningTimestampDelta = this.warningBacklog = Long.MAX_VALUE;
        } else {
            this.warningTimestampDelta = TimeUnit.MILLISECONDS.toMicros(warningTimestampDelta);
            this.warningBacklog = warningBacklog;
        }
        if (errorTimestampDelta == 0 || errorBacklog == 0) {
            this.errorTimestampDelta = this.errorBacklog = Long.MAX_VALUE;
        } else {
            this.errorTimestampDelta = TimeUnit.MILLISECONDS.toMicros(errorTimestampDelta);
            this.errorBacklog = errorBacklog;
        }
        this.rateLimitNs = TimeUnit.MILLISECONDS.toNanos(rateLimitMs);
        previouslyReported.clear();
    }

    @Override
    public Action getFirstAction() {
        return Action.procedureCall(this::handleStatisticsResult, "@Statistics", "DRPRODUCER", 0);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public Collection<String> getDependencies() {
        // This class has no external dependencies outside of java standard library and voltdb
        return Collections.emptyList();
    }

    /**
     * Callback to handle the result of the statistics query and return the next statistics call
     *
     * @param result of statistics query
     * @return next statistics action
     */
    private Action handleStatisticsResult(ActionResult result) {
        ClientResponse response = result.getResponse();

        long now = System.nanoTime();
        removeStaleReports(now);

        if (response.getStatus() == ClientResponse.SUCCESS) {
            VoltTable stats = response.getResults()[0];

            Map<Integer, LatencyReport> reports = new HashMap<>();

            while (stats.advanceRow()) {
                processStatsRow(stats, now, reports);
            }

            // Process the DR connections which exceed the configured thr
            for (LatencyReport report : reports.values()) {
                reportAboveThreshold(report);

                if (rateLimitNs > 0) {
                    previouslyReported.put(report.clusterId, new LastReported(now, report.error));
                }
            }
        }

        return Action.procedureCall(this::handleStatisticsResult, "@Statistics", "DRPRODUCER", 0);
    }

    /**
     * Report a cluster which has exceeded the configured thresholds
     *
     * @param report    holding information about the latency and which partitions have been affected
     */
    protected void reportAboveThreshold(LatencyReport report) {
        String logMessage = String.format(
                "DR has exceeded configured thresholds. Sending to cluster %d from partitions %s. Time stamp delta: %dms, backlog: %d",
                report.clusterId, report.partitionIds, TimeUnit.MICROSECONDS.toMillis(report.maxTimestampDelta),
                report.maxBacklog);

        if (report.error) {
            helper.logError(logMessage);
        } else {
            helper.logWarning(logMessage);
        }
    }

    /**
     * Remove any previously reported latency issues which are older than the rate limit
     *
     * @param now Time stamp in nanoseconds
     */
    private void removeStaleReports(long now) {
        if (!previouslyReported.isEmpty()) {
            Iterator<LastReported> iter = previouslyReported.values().iterator();
            while (iter.hasNext()) {
                LastReported lastReported = iter.next();
                if (now - lastReported.timestamp > rateLimitNs) {
                    iter.remove();
                } else {
                    return;
                }
            }
        }
    }

    /**
     * Process a single row of the statistics result
     *
     * @param stats   table pointing at the current row to analyze
     * @param now     time stamp in nanoseconds
     * @param reports Map of remoteClusterId to {@link LatencyReport} instance describing what to report
     */
    private void processStatsRow(VoltTable stats, long now, Map<Integer, LatencyReport> reports) {
        if (!("NORMAL".equalsIgnoreCase(stats.getString("MODE")) && Boolean.valueOf(stats.getString("ISSYNCED")))) {
            // DR is paused or not synced
            return;
        }

        long lastQueuedTimestamp = stats.getTimestampAsLong("LASTQUEUEDTIMESTAMP");
        long lastAckTimestamp = stats.getTimestampAsLong("LASTACKTIMESTAMP");
        long timeStampDelta = lastQueuedTimestamp - lastAckTimestamp;

        long backlog = stats.getLong("LASTQUEUEDDRID") - stats.getLong("LASTACKDRID");

        boolean error;
        if (timeStampDelta > errorTimestampDelta && backlog > errorBacklog) {
            error = true;
        } else if (timeStampDelta > warningTimestampDelta && backlog > warningBacklog) {
            error = false;
        } else {
            return;
        }

        Integer remoteClusterId = (int) stats.getLong("REMOTE_CLUSTER_ID");

        if (!shouldReport(remoteClusterId, now, error)) {
            return;
        }

        Integer partitionId = (int) stats.getLong("PARTITION_ID");
        reports.computeIfAbsent(remoteClusterId, LatencyReport::new).udpate(error, partitionId, timeStampDelta,
                backlog);
    }

    /**
     * Determine if a log message should be generated
     *
     * @param key   for the dr statistic which is over the latency threshold
     * @param now   current time stamp in nanoseconds
     * @param error {@code true} if this will be an error log
     * @return {@code true} if a log message should be generated
     */
    private boolean shouldReport(Integer remoteClusterId, long now, boolean error) {
        LastReported reported = previouslyReported.get(remoteClusterId);
        return reported == null || (error && !reported.error);
    }

    /**
     * Simple class to hold the information which should be reported
     */
    protected static final class LatencyReport {
        final int clusterId;
        boolean error = false;
        List<Integer> partitionIds = new ArrayList<>();
        long maxTimestampDelta = 0;
        long maxBacklog = 0;

        LatencyReport(int clusterId) {
            this.clusterId = clusterId;
        }

        public int getClusterId() {
            return clusterId;
        }

        public boolean isError() {
            return error;
        }

        public List<Integer> getPartitionIds() {
            return partitionIds;
        }

        public long getMaxTimestampDelta() {
            return maxTimestampDelta;
        }

        public long getMaxBacklog() {
            return maxBacklog;
        }

        /**
         * Update this report instance with information about a partition to report
         *
         * @param error          Whether or not the latency is above the error threshold
         * @param partitionId    ID of partition from which latency was measured
         * @param timestampDelta Time stamp delta in microseconds
         */
        void udpate(boolean error, Integer partitionId, long timestampDelta, long backlog) {
            this.error |= error;
            partitionIds.add(partitionId);
            maxTimestampDelta = Math.max(maxTimestampDelta, timestampDelta);
            maxBacklog = Math.max(maxBacklog, backlog);
        }
    }

    /**
     * Simple class to be the the value used in {@link #previouslyReported}
     */
    private static final class LastReported {
        final long timestamp;
        final boolean error;

        LastReported(long timestamp, boolean error) {
            super();
            this.timestamp = timestamp;
            this.error = error;
        }
    }
}
