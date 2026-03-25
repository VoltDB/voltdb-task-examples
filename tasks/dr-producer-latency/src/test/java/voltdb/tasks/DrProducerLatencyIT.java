/* This file is part of VoltDB.
 * Copyright (C) 2019-2024 VoltDB Inc.
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

import org.junit.jupiter.api.Test;
import org.voltdb.VoltTable;
import org.voltdb.client.Client2;
import org.voltdb.client.ClientResponse;
import org.voltdbtest.testcontainer.VoltDBCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for DrProducerLatency task.
 * Tests loading the task class and creating a task using CREATE TASK DDL.
 */
public class DrProducerLatencyIT extends IntegrationTestBase {

    private static final String TASK_NAME = "dr_latency_monitor";

    @Test
    public void testCreateAndValidateTask() {
        VoltDBCluster db = null;
        try {
            // Start VoltDB and load task classes
            Client2 client;
            if (isTestContainerMode()) {
                db = createTestContainer();
                startAndLoadClasses(db);
                client = db.getClient2();
            } else {
                client = createExternalClient();
                // For external mode, classes should already be loaded
            }

            // Create the task using CREATE TASK DDL
            // Parameters: warningTimestampDelta, warningBacklog, errorTimestampDelta, errorBacklog, rateLimitMs
            String createTaskDDL = String.format(
                "CREATE TASK %s ON SCHEDULE EVERY 1 MINUTES " +
                "PROCEDURE FROM CLASS voltdb.tasks.DrProducerLatency WITH (500, 500, 1000, 1000, 60000) " +
                "ON ERROR LOG",
                TASK_NAME);

            System.out.println("Creating task: " + createTaskDDL);
            ClientResponse response = client.callProcedureSync("@AdHoc", createTaskDDL);
            assertEquals(ClientResponse.SUCCESS, response.getStatus(),
                "CREATE TASK should succeed: " + response.getStatusString());

            // Verify the task was created by checking @Statistics TASK
            System.out.println("Verifying task exists...");
            response = client.callProcedureSync("@Statistics", "TASK", 0);
            assertEquals(ClientResponse.SUCCESS, response.getStatus(),
                "@Statistics TASK should succeed");

            VoltTable taskStats = response.getResults()[0];
            assertNotNull(taskStats, "Task statistics should not be null");

            boolean taskFound = false;
            while (taskStats.advanceRow()) {
                String taskName = taskStats.getString("TASK_NAME");
                if (TASK_NAME.equalsIgnoreCase(taskName)) {
                    taskFound = true;
                    String state = taskStats.getString("STATE");
                    System.out.println("Found task: " + taskName + ", state: " + state);
                    // Task should be in RUNNING state
                    assertTrue("RUNNING".equalsIgnoreCase(state) || "IDLE".equalsIgnoreCase(state),
                        "Task should be RUNNING or IDLE, but was: " + state);
                    break;
                }
            }
            assertTrue(taskFound, "Task '" + TASK_NAME + "' should exist in @Statistics TASK");

            // Test parameter validation by trying to create a task with invalid parameters
            String invalidTaskDDL = String.format(
                "CREATE TASK invalid_task ON SCHEDULE EVERY 1 MINUTES " +
                "PROCEDURE FROM CLASS voltdb.tasks.DrProducerLatency WITH (-1, 500, 1000, 1000, 60000) " +
                "ON ERROR LOG");

            System.out.println("Testing invalid parameters...");
            boolean validationFailed = false;
            try {
                response = client.callProcedureSync("@AdHoc", invalidTaskDDL);
                // If we get here without exception, check status
                validationFailed = (response.getStatus() != ClientResponse.SUCCESS);
            } catch (Exception e) {
                // VoltDB throws ProcCallException for validation failures
                validationFailed = e.getMessage().contains("Warning time stamp delta");
                System.out.println("Parameter validation correctly rejected invalid task: " + e.getMessage());
            }
            assertTrue(validationFailed, "Task with invalid parameters should fail validation");

            // Drop the task
            System.out.println("Dropping task...");
            String dropTaskDDL = "DROP TASK " + TASK_NAME;
            response = client.callProcedureSync("@AdHoc", dropTaskDDL);
            assertEquals(ClientResponse.SUCCESS, response.getStatus(),
                "DROP TASK should succeed: " + response.getStatusString());

            // Verify task was dropped
            response = client.callProcedureSync("@Statistics", "TASK", 0);
            taskStats = response.getResults()[0];
            taskFound = false;
            while (taskStats.advanceRow()) {
                if (TASK_NAME.equalsIgnoreCase(taskStats.getString("TASK_NAME"))) {
                    taskFound = true;
                    break;
                }
            }
            assertTrue(!taskFound, "Task should no longer exist after DROP TASK");

            System.out.println("\n*** DrProducerLatencyIT PASSED ***\n");

        } catch (Exception e) {
            throw new RuntimeException("Test failed", e);
        } finally {
            shutdownIfNeeded(db);
        }
    }

    @Test
    public void testValidateParametersStatic() {
        // Test the static validateParameters method directly
        // Valid parameters should return null
        String result = DrProducerLatency.validateParameters(null, 500, 500, 1000, 1000, 5000);
        assertEquals(null, result, "Valid parameters should return null");

        // Invalid warningTimestampDelta
        result = DrProducerLatency.validateParameters(null, -1, 0, 0, 0, 0);
        assertNotNull(result, "Negative warningTimestampDelta should fail");
        assertTrue(result.contains(DrProducerLatency.MSG_WARNING_TSD_POSITIVE),
            "Error message should mention warning time stamp delta");

        // Warning > Error threshold
        result = DrProducerLatency.validateParameters(null, 200, 0, 100, 0, 0);
        assertNotNull(result, "Warning > Error should fail");
        assertTrue(result.contains(DrProducerLatency.MSG_WARNING_TSD_GT_ERROR),
            "Error message should mention warning > error");

        System.out.println("*** Static validation tests PASSED ***");
    }
}
