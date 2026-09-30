# Apache Kafka with the Cruise Control metrics reporter on the classpath.
# The reporter is published to LinkedIn's JFrog, not Maven Central. It must match the Cruise Control version.
FROM apache/kafka:3.9.1
ARG REPORTER_VERSION=2.5.146
ADD --chmod=644 \
    --checksum=sha256:a62a5a97af9ed9a1744a2745f6ac8e3817953420aab575acf54bc06bb2081da0 \
    https://linkedin.jfrog.io/artifactory/cruise-control/com/linkedin/cruisecontrol/cruise-control-metrics-reporter/${REPORTER_VERSION}/cruise-control-metrics-reporter-${REPORTER_VERSION}.jar \
    /opt/kafka/libs/
