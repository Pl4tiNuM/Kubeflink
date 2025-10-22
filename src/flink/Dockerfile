FROM flink:2.1.0-java17

# 2. The official Flink distribution is located at /opt/flink.
#    We will completely replace it with our custom build.
#    First, remove the original Flink files to ensure a clean state.
RUN rm -rf /opt/flink/*

# 3. Copy your custom-built Flink distribution from the 'build-target'
#    directory on your host into the '/opt/flink' directory inside the image.
#    The 'build-target' directory was created by the 'mvn install' command.
COPY build-target /opt/flink

# 4. The official image runs as a non-root user named 'flink'. The files
#    we copied in the previous step are owned by 'root' by default.
#    We need to change the ownership so the 'flink' user has the necessary
#    permissions to execute the Flink scripts and binaries.
RUN chown -R flink:flink /opt/flink