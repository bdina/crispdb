# To build image run `docker build --tag crispdb:<version> .`

ARG GRAALVM_VERSION=22.2.0
ARG JAVA_VERSION=17
ARG GRAALVM_WORKDIR=/graalvm/src/project

ARG CRISP_VERSION=1.0.0

# Multi-stage image ... creates intermediate layer(s) for doing the graalvm native
# build (this is discarded by docker post-build)
FROM ghcr.io/graalvm/graalvm-ce:ol8-java${JAVA_VERSION}-${GRAALVM_VERSION} AS build

ARG GRADLE_VERSION=7.5.1
ARG CRISP_VERSION

WORKDIR /graalvm/src/project

# Install tools required for project
# Run `docker build --no-cache .` to update dependencies
RUN gu install native-image \
 && microdnf install -y wget unzip libstdc++-static \
 && microdnf clean all \
 && wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -P /tmp \
 && unzip -d /opt /tmp/gradle-${GRADLE_VERSION}-bin.zip \
 && rm /tmp/gradle-${GRADLE_VERSION}-bin.zip

ENV GRADLE_HOME=/opt/gradle-${GRADLE_VERSION}
ENV PATH=${GRADLE_HOME}/bin:${PATH}

# Copy the entire project and build it
# This layer is rebuilt when a file changes in the project directory
COPY . /graalvm/src/project
RUN ${GRADLE_HOME}/bin/gradle -q --no-daemon shadowJar \
 && ${JAVA_HOME}/bin/native-image \
    --static \
    -R:MinHeapSize=1m \
    -R:MaxHeapSize=1m \
    -R:MaxNewSize=1m \
    -jar build/libs/crispdb-${CRISP_VERSION}.jar

# Create a staging image (this will be part of the distribution)
#FROM oracle/graalvm-ce:${GRAALVM_VERSION} AS app-stage
#FROM alpine AS app-stage
FROM scratch AS app-stage

ARG GRAALVM_WORKDIR
ARG CRISP_VERSION

ENV CRISPDB_HOME=/opt/crispdb
ENV PATH=${CRISPDB_HOME}/bin:${PATH}

WORKDIR ${CRISPDB_HOME}

# Graal substrate VM requires libnss (even when a static binary is built)
# we copy the glibc version into the image - this is because both
# Scratch and Alpine do NOT include a glibc runtime
COPY --from=build /lib64/ld-linux-x86-64.so.2 \
                  /lib64/libc.so.6 \
                  /lib64/libnss_dns.so.2 \
                  /lib64/libnss_files.so.2 \
                  /lib64/libresolv.so.2 /lib64/

COPY --from=build ${GRAALVM_WORKDIR}/cripsdb* ${CRISPDB_HOME}/

CMD [ "/bin/sh" ]

# And we finally create the application layer
FROM app-stage AS app
ENTRYPOINT [ "./crispdb" ]
CMD [ "-XX:+PrintGC" , "-XX:+PrintGCTimeStamps" , "-XX:+VerboseGC" , "-d" ]
