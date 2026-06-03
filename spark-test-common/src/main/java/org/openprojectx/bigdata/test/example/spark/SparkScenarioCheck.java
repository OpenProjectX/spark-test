package org.openprojectx.bigdata.test.example.spark;

/** A single verification step executed against a live {@link SparkScenarioContext}. */
@FunctionalInterface
public interface SparkScenarioCheck {
    void verify(SparkScenarioContext context);
}
