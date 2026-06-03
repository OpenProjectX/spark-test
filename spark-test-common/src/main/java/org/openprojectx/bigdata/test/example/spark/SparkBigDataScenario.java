package org.openprojectx.bigdata.test.example.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.openprojectx.bigdata.test.core.BigDataEndpoint;
import org.openprojectx.bigdata.test.core.BigDataService;
import org.openprojectx.bigdata.test.core.BigDataTestKit;
import org.openprojectx.bigdata.test.extensions.core.BigDataExtensionResult;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Java port of the bigdata-test Spark JUnit 5 example. It exercises Iceberg (S3 + local GCS + HMS),
 * Hive external Parquet tables (S3 + GCS), an HDFS-backed S3 credential store, and an Avro Kafka
 * source against a Spark session wired to the bigdata-test containers.
 */
public abstract class SparkBigDataScenario {

    /** Stable URL of the fake-gcs container; {@code fake-gcs} is resolved by the java-dns agent. */
    private static final String GCS_STORAGE_ROOT_URL = "http://fake-gcs:4443/";

    protected abstract String runId();

    protected abstract String s3BucketExtensionId();

    protected abstract String gcsBucketExtensionId();

    @Test
    void runsSparkScenario(BigDataTestKit kit, BigDataExtensionResult extensions) {
        SparkScenarioEnvironment environment = createEnvironment(kit, extensions);
        try (SparkSession spark = createSparkSession(environment)) {
            SparkScenarioContext context = new SparkScenarioContext(environment, spark);
            for (SparkScenarioCheck check : infrastructureChecks()) {
                check.verify(context);
            }
            for (SparkScenarioCheck check : sourceChecks()) {
                check.verify(context);
            }
            for (SparkScenarioCheck check : targetChecks()) {
                check.verify(context);
            }
        }
    }

    protected List<SparkScenarioCheck> infrastructureChecks() {
        return List.of(
                context -> assertHdfsConfigStore(
                        context.spark(),
                        context.environment().hdfsUri(),
                        context.environment().s3CredentialProviderHdfsPath()));
    }

    protected List<SparkScenarioCheck> sourceChecks() {
        return List.of(
                context -> assertKafkaAvroInput(
                        context.spark(),
                        context.environment().kafkaBootstrapServers(),
                        kafkaAvroTopic(),
                        context.environment().kafkaSecurityProtocol(),
                        context.environment().kafkaKerberosServiceName(),
                        context.environment().kafkaJaasConfig(),
                        context.environment().kafkaSslProperties()));
    }

    protected List<SparkScenarioCheck> targetChecks() {
        String runId = runId();
        return List.of(
                context -> assertIcebergTable(
                        context.spark(), "s3", "demo_" + runId, "events_s3", "s3", null),
                context -> assertIcebergTable(
                        context.spark(), "gcs_local", "demo_" + runId, "events_gcs", "gcs",
                        context.environment().gcsIcebergDataPath()),
                context -> {
                    assertIcebergTable(
                            context.spark(), "hms", "hms_demo_" + runId, "events_hms", "hms", null);
                    assertHiveMetastoreTable(
                            context.environment().hiveMetastoreUri(),
                            context.environment().hiveMetastoreTlsProperties(),
                            "hms_demo_" + runId,
                            "events_hms");
                },
                context -> assertHiveExternalParquetTable(
                        context.spark(),
                        context.environment().hiveMetastoreUri(),
                        context.environment().hiveMetastoreTlsProperties(),
                        "hive_s3_demo_" + runId,
                        "events_parquet_s3",
                        "s3a://" + context.environment().s3Bucket() + "/hive-parquet/events_parquet_s3",
                        "hive-s3-parquet",
                        true),
                context -> assertHiveExternalParquetTable(
                        context.spark(),
                        context.environment().hiveMetastoreUri(),
                        context.environment().hiveMetastoreTlsProperties(),
                        "hive_gcs_demo_" + runId,
                        "events_parquet_gcs",
                        context.environment().gcsIcebergDataPath(),
                        "gcs",
                        false));
    }

    protected String kafkaAvroTopic() {
        return "spark-avro-events";
    }

    private SparkScenarioEnvironment createEnvironment(BigDataTestKit kit, BigDataExtensionResult extensions) {
        BigDataEndpoint hdfs = kit.endpoint(BigDataService.HDFS);
        BigDataEndpoint hiveMetastore = kit.endpoint(BigDataService.HIVE_METASTORE);
        BigDataEndpoint kafka = kit.endpoint(BigDataService.KAFKA);
        BigDataEndpoint s3 = kit.endpoint(BigDataService.LOCALSTACK_S3);
        Map<String, String> kafkaProperties = kafka.getProperties();

        String krb5Conf = extensions.optional("kerberos-material.krb5-conf");
        if (krb5Conf == null) {
            krb5Conf = kafkaProperties.get("java.security.krb5.conf.local");
        }
        String kafkaKerberosServiceName = extensions.optional("kerberos-material.kafka.service-name");
        if (kafkaKerberosServiceName == null) {
            kafkaKerberosServiceName = kafkaProperties.get("sasl.kerberos.service.name");
        }

        return new SparkScenarioEnvironment(
                runId(),
                hdfs.property("fs.defaultFS"),
                hiveMetastore.property("hive.metastore.uris"),
                kafka.property("bootstrap.servers"),
                s3.property("aws.endpoint-url.s3"),
                extensions.required(s3BucketExtensionId() + ".bucket"),
                extensions.required(gcsBucketExtensionId() + ".bucket"),
                extensions.required(gcsBucketExtensionId() + ".gs.uri") + "/data/demo_" + runId() + "/events_gcs",
                extensions.required("s3-jceks.credential-provider.path"),
                extensions.required("s3-jceks.hdfs.path"),
                extensions.optional("kerberos-material.client.principal"),
                extensions.optional("kerberos-material.client.password"),
                extensions.optional("kerberos-material.client.keytab"),
                krb5Conf,
                kafkaProperties.get("security.protocol"),
                kafkaKerberosServiceName,
                kafkaProperties.get("sasl.jaas.config"),
                filterKeys(kafkaProperties, key -> key.startsWith("ssl.")),
                filterKeys(hiveMetastore.getProperties(), key ->
                        key.equals("hive.metastore.use.SSL")
                                || key.equals("hive.metastore.truststore.path")
                                || key.equals("hive.metastore.truststore.password")));
    }

    private SparkSession createSparkSession(SparkScenarioEnvironment environment) {
        if (environment.krb5Conf() != null) {
            System.setProperty("java.security.krb5.conf", environment.krb5Conf());
        }
        SparkSession.Builder builder = SparkSession.builder()
                .appName("bigdata-test-spark-example")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config(
                        "spark.driver.extraJavaOptions",
                        "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED")
                .config(
                        "spark.executor.extraJavaOptions",
                        "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.s3", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.s3.type", "hadoop")
                .config("spark.sql.catalog.s3.warehouse", "s3a://" + environment.s3Bucket() + "/warehouse")
                .config("spark.sql.catalog.gcs_local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.gcs_local.type", "hadoop")
                .config("spark.sql.catalog.gcs_local.warehouse",
                        "file:" + createTempDir("bigdata-test-gcs-iceberg-warehouse-"))
                .config("spark.sql.catalog.hms", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hms.type", "hive")
                .config("spark.sql.catalog.hms.uri", environment.hiveMetastoreUri())
                .config("spark.sql.catalog.hms.warehouse", "file:" + createTempDir("bigdata-test-hms-warehouse-"))
                .config("spark.sql.warehouse.dir", "file:" + createTempDir("bigdata-test-spark-warehouse-"))
                .config("spark.sql.statistics.size.autoUpdate.enabled", "false")
                .config("hive.metastore.uris", environment.hiveMetastoreUri())
                .config("spark.hadoop.fs.defaultFS", environment.hdfsUri())
                .config("spark.hadoop.hadoop.security.credential.provider.path", environment.s3CredentialProviderPath())
                .config("spark.hadoop.fs.s3a.endpoint", environment.s3Endpoint())
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.hadoop.fs.s3a.access.key", "test")
                .config("spark.hadoop.fs.s3a.secret.key", "test")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                .config("spark.hadoop.fs.gs.project.id", "bigdata-test")
                // The fake-gcs host is resolved to the local container by the java-dns agent,
                // so a fixed URL is used instead of the dynamic container endpoint.
                .config("spark.hadoop.fs.gs.storage.root.url", GCS_STORAGE_ROOT_URL)
                .config("spark.hadoop.fs.gs.storage.service.path", "storage/v1/")
                .config("spark.hadoop.fs.gs.client.type", "HTTP_API_CLIENT")
                .config("spark.hadoop.fs.gs.http.connect-timeout", "4000")
                .config("spark.hadoop.fs.gs.auth.type", "UNAUTHENTICATED")
                .config("spark.hadoop.fs.gs.status.parallel.enable", "false")
                .config("spark.hadoop.fs.gs.create.items.conflict.check.enable", "false")
                .config("spark.hadoop.fs.gs.implicit.dir.repair.enable", "false")
                .config("spark.hadoop.fs.gs.hierarchical.namespace.folders.enable", "false");

        configureHiveMetastoreTls(builder, environment);
        configureKerberos(builder, environment);
        builder = configureSpark(builder, environment);
        return builder.enableHiveSupport().getOrCreate();
    }

    /** Hook for subclasses to further customise the Spark session builder. */
    protected SparkSession.Builder configureSpark(SparkSession.Builder builder, SparkScenarioEnvironment environment) {
        return builder;
    }

    private void configureKerberos(SparkSession.Builder builder, SparkScenarioEnvironment environment) {
        if (environment.krb5Conf() != null) {
            builder.config("spark.hadoop.java.security.krb5.conf", environment.krb5Conf());
        }
        if (environment.kerberosClientPrincipal() != null) {
            builder.config("spark.hadoop.bigdata.test.kerberos.client.principal", environment.kerberosClientPrincipal());
        }
        if (environment.kerberosClientKeytab() != null) {
            builder.config("spark.hadoop.bigdata.test.kerberos.client.keytab", environment.kerberosClientKeytab());
        }
        if (environment.kafkaKerberosServiceName() != null) {
            builder.config("spark.hadoop.bigdata.test.kafka.service.name", environment.kafkaKerberosServiceName());
        }
        if (environment.kafkaJaasConfig() != null) {
            builder.config("spark.hadoop.bigdata.test.kafka.jaas.config", environment.kafkaJaasConfig());
        }
    }

    private void configureHiveMetastoreTls(SparkSession.Builder builder, SparkScenarioEnvironment environment) {
        environment.hiveMetastoreTlsProperties().forEach((key, value) -> {
            builder.config(key, value);
            builder.config("spark.hadoop." + key, value);
        });
    }

    protected void assertHdfsConfigStore(SparkSession spark, String hdfsUri, String hdfsPath) {
        boolean exists;
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        try (FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf)) {
            exists = fs.exists(new Path(hdfsPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        check(exists, () -> "Expected S3 JCEKS file in HDFS for " + spark.sparkContext().appName());
    }

    protected void assertKafkaAvroInput(
            SparkSession spark,
            String bootstrapServers,
            String topic,
            String securityProtocol,
            String kerberosServiceName,
            String jaasConfig,
            Map<String, String> sslProperties) {
        DataFrameReader reader = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest");

        if (securityProtocol != null) {
            reader.option("kafka.security.protocol", securityProtocol);
        }
        sslProperties.forEach((key, value) -> reader.option("kafka." + key, value));
        if ("SASL_PLAINTEXT".equals(securityProtocol) || "SASL_SSL".equals(securityProtocol)) {
            reader.option("kafka.sasl.mechanism", "GSSAPI")
                    .option("kafka.sasl.kerberos.service.name", kerberosServiceName != null ? kerberosServiceName : "kafka")
                    .option("kafka.sasl.jaas.config", jaasConfig != null ? jaasConfig : "");
        }

        long count = reader.load().count();
        check(count == 2L, () -> "Expected two Avro Kafka records in " + topic);
    }

    protected void assertIcebergTable(
            SparkSession spark,
            String catalog,
            String namespace,
            String table,
            String storageName,
            String dataPath) {
        String identifier = catalog + "." + namespace + "." + table;
        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalog + "." + namespace);
        String tableProperties = icebergTableProperties(Map.entry("write.data.path", dataPath == null ? "" : dataPath), dataPath != null);
        spark.sql(
                "CREATE TABLE " + identifier + " (\n"
                        + "    id INT,\n"
                        + "    name STRING,\n"
                        + "    storage STRING\n"
                        + ") USING iceberg" + tableProperties);
        spark.sql("INSERT INTO " + identifier + " VALUES (1, 'alpha', '" + storageName + "'), (2, 'beta', '" + storageName + "')");
        long count = spark.table(identifier).where("storage = '" + storageName + "'").count();
        check(count == 2L, () -> "Expected two Iceberg rows in " + identifier);
    }

    protected void assertHiveMetastoreTable(
            String hiveMetastoreUri,
            Map<String, String> hiveMetastoreTlsProperties,
            String database,
            String table) {
        HiveConf conf = hiveMetastoreConf(hiveMetastoreUri, hiveMetastoreTlsProperties);
        IMetaStoreClient client = hiveMetastoreClient(conf);
        try {
            check(client.getAllDatabases().contains(database), () -> "Expected HMS database " + database);
            Table hmsTable = client.getTable(database, table);
            check(hmsTable.getDbName().equals(database),
                    () -> "Expected HMS table " + database + "." + table + ", got " + hmsTable.getDbName() + "." + hmsTable.getTableName());
            check(hmsTable.getTableName().equals(table),
                    () -> "Expected HMS table " + database + "." + table + ", got " + hmsTable.getDbName() + "." + hmsTable.getTableName());
            check("ICEBERG".equals(hmsTable.getParameters().get("table_type")),
                    () -> "Expected HMS table " + database + "." + table + " to be an Iceberg table, parameters=" + hmsTable.getParameters());
            check(hmsTable.getParameters().containsKey("metadata_location"),
                    () -> "Expected HMS table " + database + "." + table + " to contain Iceberg metadata_location");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            close(client);
        }
    }

    protected void assertHiveExternalParquetTable(
            SparkSession spark,
            String hiveMetastoreUri,
            Map<String, String> hiveMetastoreTlsProperties,
            String database,
            String table,
            String location,
            String storageName,
            boolean seedData) {
        String identifier = database + "." + table;
        spark.sql("CREATE DATABASE IF NOT EXISTS " + database);
        if (seedData) {
            spark.sql(
                    "SELECT 1 AS id, 'alpha' AS name, '" + storageName + "' AS storage\n"
                            + "UNION ALL\n"
                            + "SELECT 2 AS id, 'beta' AS name, '" + storageName + "' AS storage")
                    .write().mode("overwrite").parquet(location);
        }
        spark.sql(
                "CREATE EXTERNAL TABLE " + identifier + " (\n"
                        + "    id INT,\n"
                        + "    name STRING,\n"
                        + "    storage STRING\n"
                        + ") STORED AS PARQUET\n"
                        + "LOCATION '" + location + "'");
        long count = spark.table(identifier).where("storage = '" + storageName + "'").count();
        check(count == 2L, () -> "Expected two Hive external Parquet rows in " + identifier);
        assertParquetFiles(spark, location);
        assertHiveMetastoreExternalParquetTable(hiveMetastoreUri, hiveMetastoreTlsProperties, database, table, location);
    }

    private void assertParquetFiles(SparkSession spark, String location) {
        List<String> files = new ArrayList<>();
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        try (FileSystem fs = FileSystem.get(URI.create(location), conf)) {
            for (FileStatus status : fs.listStatus(new Path(location))) {
                files.add(status.getPath().getName());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        check(files.stream().anyMatch(name -> name.endsWith(".parquet")),
                () -> "Expected Parquet files under " + location + ", got " + files);
    }

    private void assertHiveMetastoreExternalParquetTable(
            String hiveMetastoreUri,
            Map<String, String> hiveMetastoreTlsProperties,
            String database,
            String table,
            String location) {
        HiveConf conf = hiveMetastoreConf(hiveMetastoreUri, hiveMetastoreTlsProperties);
        IMetaStoreClient client = hiveMetastoreClient(conf);
        try {
            Table hmsTable = client.getTable(database, table);
            check(hmsTable.getDbName().equals(database),
                    () -> "Expected HMS table " + database + "." + table + ", got " + hmsTable.getDbName() + "." + hmsTable.getTableName());
            check(hmsTable.getTableName().equals(table),
                    () -> "Expected HMS table " + database + "." + table + ", got " + hmsTable.getDbName() + "." + hmsTable.getTableName());
            check(location.equals(hmsTable.getSd().getLocation()),
                    () -> "Expected HMS table " + database + "." + table + " location " + location + ", got " + hmsTable.getSd().getLocation());
            check(hmsTable.getSd().getInputFormat().contains("Parquet"),
                    () -> "Expected HMS table " + database + "." + table + " to be Parquet, inputFormat=" + hmsTable.getSd().getInputFormat());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            close(client);
        }
    }

    private IMetaStoreClient hiveMetastoreClient(HiveConf conf) {
        Constructor<?> constructor = null;
        for (Class<?> parameterType : List.of(HiveConf.class, Configuration.class)) {
            try {
                constructor = HiveMetaStoreClient.class.getConstructor(parameterType);
                break;
            } catch (NoSuchMethodException ignored) {
                // try the next supported constructor
            }
        }
        if (constructor == null) {
            throw new IllegalStateException("No supported HiveMetaStoreClient constructor found");
        }
        try {
            return (IMetaStoreClient) constructor.newInstance(conf);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private HiveConf hiveMetastoreConf(String hiveMetastoreUri, Map<String, String> hiveMetastoreTlsProperties) {
        HiveConf conf = new HiveConf();
        conf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetastoreUri);
        hiveMetastoreTlsProperties.forEach(conf::set);
        return conf;
    }

    private String icebergTableProperties(Map.Entry<String, String> property, boolean present) {
        if (!present) {
            return "";
        }
        return " TBLPROPERTIES ('" + property.getKey() + "'='" + property.getValue() + "')";
    }

    private static String createTempDir(String prefix) {
        try {
            return Files.createTempDirectory(prefix).toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> filterKeys(Map<String, String> source, java.util.function.Predicate<String> keep) {
        Map<String, String> result = new LinkedHashMap<>();
        source.forEach((key, value) -> {
            if (keep.test(key)) {
                result.put(key, value);
            }
        });
        return result;
    }

    private static void check(boolean condition, java.util.function.Supplier<String> message) {
        if (!condition) {
            throw new IllegalStateException(message.get());
        }
    }

    private static void close(IMetaStoreClient client) {
        if (client != null) {
            client.close();
        }
    }
}
