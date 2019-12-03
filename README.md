# voltdb-task-examples
This repository contains several example custom tasks. Each task is in its own sub directory under the main tasks
directory.  These examples can be used as-is or modified to fit your needs.
## Using an example
1. Clone this repository
1. Modify the example task if desired
1. Build the task
1. Update the classes on a running VoltDB instance. [Documentation](https://docs.voltdb.com/UsingVoltDB/sysprocupdateclasses.php)
1. Create the task in VoltDB. [Documentation](https://docs.voltdb.com/UsingVoltDB/ddlref_createtask.php)
## Building
gradle is used as the build tool. The gradle wrapper can be used by executing `gradlew` or `gradlew.bat` in the root of the project.
There is a top level jar task which generates a fat jar which includes all of the tasks.
### Build and test all tasks
`./gradlew build`
### Build all jars
Including jars for each task as well as the fat jar containing all tasks.

`./gradlew jar`
### Build and test a single task
`./gradlew :taskName:build`
