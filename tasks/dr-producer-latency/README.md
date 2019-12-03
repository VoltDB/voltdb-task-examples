# dr-producer-latency task
This is an example of a task which executes `@Statistics DRPRODUCER 0` and logs a warning when the latency observed in
the statistics is greater than a given threshold. The example implements the ActionGenerator interface which provides
the procedure to execute and then analyzes the result of that procedure.

The implementation takes these parameters

Parameter | Description
--------- | -----------
Warning time stamp delta | The time stamp delta threshold, in milliseconds, at which point a warning message will be logged if warning backlog is also exceeded. If 0 threshold is disabled. The delta is calculated by subtracting LASTACKTIMESTAMP from LASTQUEUEDTIMESTAMP
Warning backlog | The backlog threshold for number of outstanding transactions to replicate. If the threshold is exceeded and the warning time stamp delta is exceeded a warning message will be logged. If 0 threshold is disabled. The backlog is calculated by subtracting LASTACKDRID from LASTQUEUEDDRID
Error time stamp delta | The time stamp delta threshold, in milliseconds, at which point an error message will be logged if error backlog is also exceeded. If 0 threshold is disabled. The delta is calculated by subtracting LASTACKTIMESTAMP from LASTQUEUEDTIMESTAMP
Error backlog | The backlog threshold for number of outstanding transactions to replicate. If the threshold is exceeded and the error time stamp delta is exceeded a warning message will be logged. If 0 threshold is disabled. The backlog is calculated by subtracting LASTACKDRID from LASTQUEUEDDRID

## Build and install
1. `../../gradlew :drProducerLatency:jar`
1. sqlcmd
   1. `load classes build/libs/tasks-dr-producer-latency-1.0.0.jar;`
   1. `CREATE TASK dr-producer-latency ON SCHEDULE <schedule> PROCEDURE FROM CLASS voltdb.tasks.DrProducerLatency WITH (<warning time stamp delta>, <warning backlog>, <error time stamp delta>, <error backlog>, <rate limit>);`

[Create Task Documentation](https://docs.voltdb.com/UsingVoltDB/ddlref_createtask.php)
