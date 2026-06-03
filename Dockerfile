# Builds the whole project and KEEPS everything in the final image:
#   - every downloaded Maven dependency        -> /root/.m2/repository
#   - every built module jar / build artifact  -> /workspace/spark-test/**/target
#   - the project source code                  -> /workspace/spark-test
#
# The source is copied as-is except for the files excluded by .dockerignore
# (which mirrors .gitignore), so generated/IDE/VCS files are not brought in.
#
# Tests are NOT run during the image build: the scenario needs a Docker daemon
# (Testcontainers), which is not available inside an image build. Dependencies and
# jars are still fully resolved and produced.
FROM maven:3.9-eclipse-temurin-17

# Optional proxy support for local builds behind an HTTP proxy (empty in CI).
# Example: docker build --build-arg HTTPS_PROXY=http://host.docker.internal:10809 .
ARG HTTP_PROXY=""
ARG HTTPS_PROXY=""
ARG NO_PROXY=""
ENV HTTP_PROXY=${HTTP_PROXY} HTTPS_PROXY=${HTTPS_PROXY} NO_PROXY=${NO_PROXY} \
    http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} no_proxy=${NO_PROXY}

WORKDIR /workspace/spark-test

# Copy the project source (ignored files are filtered out by .dockerignore).
COPY . .

# Archive the (gitignore-filtered) source into the image as a tarball, before building
# so it holds pure source with no target/ build output.
RUN tar --exclude='*/target' -czf /workspace/spark-test-src.tgz -C /workspace spark-test

# Install all dependencies and build the module jars; keep the local repo + target dirs.
# Do NOT use a cache mount here on purpose: the downloaded deps must persist in the image.
RUN mvn -B -e install -DskipTests

CMD ["bash"]
