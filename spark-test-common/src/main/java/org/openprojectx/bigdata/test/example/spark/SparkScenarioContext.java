package org.openprojectx.bigdata.test.example.spark;

import org.apache.spark.sql.SparkSession;

/** Holds the resolved environment and the active Spark session for one scenario run. */
public record SparkScenarioContext(SparkScenarioEnvironment environment, SparkSession spark) {
}
