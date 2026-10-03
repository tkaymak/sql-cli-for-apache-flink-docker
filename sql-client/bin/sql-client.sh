#!/bin/bash
# Starts the Flink SQL client against the jobmanager; all connector jars are shipped with each job.
exec "${FLINK_HOME}/bin/sql-client.sh" embedded -l "file://${SQL_CLIENT_HOME}/lib" "$@"
