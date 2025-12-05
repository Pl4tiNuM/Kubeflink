FROM ubuntu:24.04 AS source

RUN apt-get update && apt-get install -y git

WORKDIR /
RUN git clone https://github.com/apache/flink.git
WORKDIR /flink
RUN git checkout release-2.1.0


FROM source AS flink-base

RUN apt-get update && apt-get install -y yq openjdk-17-jre-headless curl

ENV DEBIAN_FRONTEND=noninteractive \
    MAVEN_VERSION=3.8.6 \
    MAVEN_HOME=/opt/maven

RUN curl -fsSL -o /tmp/maven.tgz \
      https://archive.apache.org/dist/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.tar.gz \
 && mkdir -p /opt \
 && tar -xzf /tmp/maven.tgz -C /opt \
 && ln -s /opt/apache-maven-3.8.6 ${MAVEN_HOME} \
 && ln -s ${MAVEN_HOME}/bin/mvn /usr/local/bin/mvn \
 && rm /tmp/maven.tgz

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN ./mvnw clean package -DskipTests -Djdk17 -Pjava17-target

FROM flink-base AS flink-custom

COPY patches/flink-kubernetes/src/main/java/org/apache/flink/kubernetes/KubernetesResourceManagerDriver.java /flink/flink-kubernetes/src/main/java/org/apache/flink/kubernetes/KubernetesResourceManagerDriver.java

RUN mvn spotless:apply
RUN ./mvnw clean package -DskipTests -Dcheckstyle.skip -Drat.skip=true -Djdk17 -Pjava17-target  -pl flink-runtime,flink-kubernetes -am

FROM flink:2.1.0-java17

# 2. The official Flink distribution is located at /opt/flink.
#    We will completely replace it with our custom build.
#    First, remove the original Flink files to ensure a clean state.
RUN rm -rf /opt/flink/*

# 3. Copy your custom-built Flink distribution from the 'build-target'
#    directory on your host into the '/opt/flink' directory inside the image.
#    The 'build-target' directory was created by the 'mvn install' command.
COPY --from=flink-custom /flink/build-target/ /opt/flink/

# 4. The official image runs as a non-root user named 'flink'. The files
#    we copied in the previous step are owned by 'root' by default.
#    We need to change the ownership so the 'flink' user has the necessary
#    permissions to execute the Flink scripts and binaries.
RUN chown -R flink:flink /opt/flink
