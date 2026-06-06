# spark-test

A Maven + Java port of the bigdata-test Spark JUnit 5 example
([`bigdata-test/example/spark`](../bigdata-test/example/spark)). It runs the same end-to-end
Spark scenario against **two dependency lines** — Apache Spark/Hadoop and Cloudera
Spark/Hadoop — using the [`bigdata-test`](../bigdata-test) framework published locally as
`0.1.1-SNAPSHOT`.

## What the test does

`SparkBigDataScenario` spins up the bigdata-test containers (HDFS, Hive Metastore, Kafka,
LocalStack S3, fake-gcs, Kerberos KDC) and a local Spark session, then verifies:

- an HDFS-backed S3 JCEKS credential store exists;
- an Avro Kafka source can be read (Kerberos/SASL + TLS aware);
- Iceberg tables can be created and queried on **S3**, a **local GCS** warehouse, and the **HMS**
  catalog (with the Iceberg table also asserted directly against the Hive Metastore);
- Hive **external Parquet** tables can be written/read on **S3** and **GCS**, asserted against the
  Hive Metastore.

The concrete `SparkBigDataTestExample` runs with the `cloudera-hms-kerberos` configuration, matching
the default test in the original Gradle example.

## Module layout

| Module               | Purpose                                                                                   |
|----------------------|-------------------------------------------------------------------------------------------|
| `spark-test-bom`     | BOM that manages the `org.openprojectx.*` dependency versions (bigdata-test + java-dns) in one place; the other modules import it. |
| `spark-test-common`  | The shared scenario, the `SparkBigDataTestExample` test class, and the test resources (TOML/Avro/log4j2). Holds the test code exactly once. |
| `spark-apache`       | Runs the shared test against the **Apache** line (Spark 3.5.7 / Hadoop 3.4.2 / Iceberg 1.11.0). |
| `spark-cloudera`     | Runs the shared test against the **Cloudera** line (Spark 3.3.2.3.3.7190.9-1 / Hadoop 3.1.1.7.1.9.14-2 / Iceberg 1.8.1). |

`spark-test-common` declares its Spark/Hadoop dependencies as `provided` (compile-only). Each
runtime module supplies the real versions and re-runs the shared test via the Surefire
[`dependenciesToScan`](https://maven.apache.org/surefire/maven-surefire-plugin/examples/inclusion-exclusion.html)
mechanism, so the two lines never collide on the classpath.

The dependency versions were taken from the original Gradle project:

```bash
GRADLE_USER_HOME=/data/.gradle ./gradlew :example:spark:dependencies \
  --configuration apacheSparkRuntimeClasspath
GRADLE_USER_HOME=/data/.gradle ./gradlew :example:spark:dependencies \
  --configuration clouderaSparkRuntimeClasspath
```

## Plugins

- **[hadoop-native-loader](../hadoop-native-loader) `0.1.4`** — its `extract` goal unpacks the
  bundled Hadoop native libraries and prepends `-Djava.library.path` / `-Dhadoop.home.dir` to the
  Surefire `argLine`, so the native Hadoop code is used instead of the pure-Java fallback.
- **[java-dns](../java-dns) `0.1.1`** — the agent jar (`org.openprojectx.java.dns:core`) is attached
  to the Surefire JVM via `-javaagent:…=hostsFile=${project.basedir}/dns.hosts`. The
  [`dns.hosts`](spark-apache/dns.hosts) file maps the bigdata-test container network aliases
  (`fake-gcs`, `hdfs`) to `127.0.0.1`, so the clients reach the local mapped containers without
  wiring per-run endpoints into the Spark session. In particular the GCS connector can use the
  stable URL `http://fake-gcs:4443/` and the test **no longer needs to read the dynamic GCS
  endpoint**. The fake-gcs container is bound to the fixed host port `4443` via
  `[ports] fakeGcs = 4443` in `spark-bigdata-test-common.toml` so the redirected host:port lines up.

## Prerequisites

- JDK 17
- Maven 3.9.x
- Docker (Testcontainers spins up the bigdata-test services)
- The `bigdata-test` framework installed locally as `0.1.1-SNAPSHOT`
  (`./gradlew publishToMavenLocal` in the bigdata-test checkout). The `java-dns` and
  `hadoop-native-loader` released plugin versions resolve from the configured remote repositories.

This build honours your user `~/.m2/settings.xml` (proxy + mirrors); run Maven outside any sandbox
so it can reach the network.

## Dependency convergence notes

Gradle resolves version conflicts to the **highest** requested version; Maven uses **nearest-wins**.
A few places therefore need an explicit pin/exclusion in Maven that Gradle handled automatically:

- **`jackson-annotations` → 2.15.2** (parent `dependencyManagement`). Cloudera Spark requests
  `2.12.7`, whose `JsonFormat.Feature` lacks `READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE` that
  Testcontainers 2.0.4's shaded Jackson needs. 2.15.2 is Apache Spark 3.5's Jackson line.
  `jackson-databind`/`jackson-core` are left at each Spark line's own version.
- **`spark-apache`: pin `slf4j-api` → 2.0.17.** The Apache line pulls both the SLF4J 1.7 binding
  (`log4j-slf4j-impl`) and the 2.0 binding (`log4j-slf4j2-impl`); nearest-wins would keep
  `slf4j-api:1.7.25` and the 2.0 binding then fails on `org.slf4j.spi.LoggingEventBuilder`.
- **`spark-cloudera`: exclude `hadoop-client-api`/`hadoop-client-runtime` from `extensions`.**
  Those are Apache Hadoop 3.4.2 shaded clients whose `core-default.xml` uses duration strings
  (`fs.s3a.threads.keepalivetime=60s`) that Cloudera's Hadoop 3.1.1 S3A code parses as a plain
  number. Excluding them leaves Cloudera Spark's own Hadoop 3.1.1 as the single Hadoop on the line.

Both modules pass the full scenario (`Tests run: 1, Failures: 0, Errors: 0`) against a local Docker.

## Build & run

Maven modules depend on built artifacts, not directly on sibling source or resource directories. If
you run a single runtime module without `-am`, Maven resolves `spark-test-common` from
`~/.m2/repository`, so stale TOML/resources can be used until `spark-test-common` is installed
again. Prefer `-am` for focused module runs; it adds required reactor modules and uses the freshly
built `spark-test-common` artifact.

```bash
# compile everything and install spark-test-common into the local repo
mvn install -DskipTests

# run the full scenario on one line and also build required reactor modules
mvn -pl spark-apache -am test
mvn -pl spark-cloudera -am test

# only use this after mvn install has refreshed local SNAPSHOT artifacts
mvn -pl spark-cloudera test

# or both
mvn test
```

## Docker image

The [`Dockerfile`](Dockerfile) builds the whole project inside the image and **keeps everything**:
all downloaded Maven dependencies (`/root/.m2/repository`), every built module jar
(`**/target/*.jar`), the project source (`/workspace/spark-test`), and a source tarball
(`/workspace/spark-test-src.tgz`, captured before the build so it is pure source with no
`target/`). The source is copied except for the files excluded by [`.dockerignore`](.dockerignore),
which mirrors `.gitignore` (so `target/`, `.idea/`, `.git`, etc. are not brought in). Tests are
skipped during the image build because Testcontainers needs a Docker daemon that isn't available
there.

```bash
docker build -t spark-test .

# Behind an HTTP proxy (e.g. local dev), pass it through:
docker build --build-arg HTTPS_PROXY=http://host.docker.internal:10809 -t spark-test .
```

### Slim image ([`Dockerfile.slim`](Dockerfile.slim))

A lightweight image that pre-populates the Maven local repo with **only the `org.openprojectx.*`
artifacts** the project uses — the bigdata-test framework and its transitive openprojectx modules
(`junit5`, `extensions`, `core`, `hive-docker-testcontainers`), the `java-dns` agent, and the
`hadoop-native-loader` Maven plugin (+ its native-libs `core`). It does **not** download Spark,
Hadoop, or any other third-party jars: the set is discovered from the dependency graph and each
artifact is fetched with `-Dtransitive=false` (each main jar plus its `-sources` jar). The list is
dynamic, so version bumps (e.g. `bigdata-test.version`) are picked up automatically.

```bash
docker build -f Dockerfile.slim -t spark-test-openprojectx .
```

## CI / GitHub Container Registry

[`.github/workflows/build.yml`](.github/workflows/build.yml) publishes to **GitHub Container
Registry** (`ghcr.io/<owner>/spark-test`). By **default it builds and pushes only the
[slim image](#slim-image-dockerfileslim)** (`-slim`-suffixed tags: `latest-slim`, `<short-sha>-slim`,
…). The full image (the heavy full Maven build) is **opt-in and disabled by default** — trigger the
workflow manually (`workflow_dispatch`) with the **`build_full`** input enabled to also build and
push it. It needs no extra secrets — it authenticates with the built-in `GITHUB_TOKEN`
(`packages: write`). Pull requests build the image(s) but do not push.

[`.github/workflows/windows.yml`](.github/workflows/windows.yml) runs on `windows-latest` to check
Windows compatibility: it builds every module with Maven and verifies the `hadoop-native-loader`
plugin extracts the Windows-native Hadoop artifacts (`winutils.exe`, `hadoop.dll`). The full
scenario test is not run there — it needs Testcontainers (Linux containers), which GitHub-hosted
Windows runners can't run; run `mvn test` on Linux, or on a self-hosted Windows machine backed by a
Linux-container Docker engine.
