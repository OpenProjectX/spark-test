package org.openprojectx.bigdata.test.example.spark;

import java.util.Map;

/**
 * Immutable view of everything the scenario needs from the bigdata-test kit and extensions.
 *
 * <p>Note: unlike the original Gradle example there is no {@code gcsEndpoint} field. The fake-gcs
 * container is reached through the stable URL {@code http://fake-gcs:4443/}; the {@code fake-gcs}
 * host name is resolved to the local container by the java-dns agent attached to the test JVM, so
 * the test never has to read the dynamic GCS endpoint.
 */
public record SparkScenarioEnvironment(
        String runId,
        String hdfsUri,
        String hiveMetastoreUri,
        String kafkaBootstrapServers,
        String s3Endpoint,
        String s3Bucket,
        String gcsBucket,
        String gcsIcebergDataPath,
        String s3CredentialProviderPath,
        String s3CredentialProviderHdfsPath,
        String kerberosClientPrincipal,
        String kerberosClientPassword,
        String kerberosClientKeytab,
        String krb5Conf,
        String kafkaSecurityProtocol,
        String kafkaKerberosServiceName,
        String kafkaJaasConfig,
        Map<String, String> kafkaSslProperties,
        Map<String, String> hiveMetastoreTlsProperties) {
}
