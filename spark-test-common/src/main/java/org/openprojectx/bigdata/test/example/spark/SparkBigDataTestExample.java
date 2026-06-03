package org.openprojectx.bigdata.test.example.spark;

import org.openprojectx.bigdata.test.extensions.config.BigDataExtensionsBuilder;
import org.openprojectx.bigdata.test.extensions.config.BigDataExtensionsConfigurer;
import org.openprojectx.bigdata.test.extensions.junit5.BigDataExtensions;
import org.openprojectx.bigdata.test.junit5.BigDataTest;

/**
 * Concrete Spark example test. The two runtime modules execute this exact class (and the shared
 * {@link SparkBigDataScenario} test method) against their own Spark/Hadoop dependency lines.
 */
@BigDataExtensions(value = "classpath:spark-bigdata-extensions.toml", configurer = SparkBigDataTestExample.Configurer.class)
@BigDataTest(config = {
        "classpath:spark-bigdata-test-common.toml",
        "classpath:spark-bigdata-test-cloudera-hms-kerberos.toml",
})
public class SparkBigDataTestExample extends SparkBigDataScenario {

    static final String SCENARIO_RUN_ID = Long.toString(System.nanoTime());
    static final String S3_BUCKET_ID = "spark-s3-bucket";
    static final String GCS_BUCKET_ID = "spark-gcs-bucket";

    @Override
    protected String runId() {
        return SCENARIO_RUN_ID;
    }

    @Override
    protected String s3BucketExtensionId() {
        return S3_BUCKET_ID;
    }

    @Override
    protected String gcsBucketExtensionId() {
        return GCS_BUCKET_ID;
    }

    /** Registers the S3 and GCS buckets the scenario writes to. */
    public static class Configurer implements BigDataExtensionsConfigurer {
        @Override
        public void configure(BigDataExtensionsBuilder extensions) {
            extensions.s3Bucket("spark-iceberg-s3-" + SCENARIO_RUN_ID, S3_BUCKET_ID);
            extensions.gcsBucket("spark-iceberg-gcs-" + SCENARIO_RUN_ID, GCS_BUCKET_ID);
        }
    }
}
