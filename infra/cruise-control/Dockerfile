FROM eclipse-temurin:17-jdk-jammy AS build
RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /src
ARG CC_REF=2.5.146
RUN git clone --depth 1 --branch ${CC_REF} https://github.com/cruise-control-for-kafka/cruise-control.git .
RUN ./gradlew jar copyDependantLibs --no-daemon

FROM eclipse-temurin:17-jre-jammy
WORKDIR /opt/cruise-control
COPY --from=build /src/kafka-cruise-control-start.sh /opt/cruise-control/kafka-cruise-control-start.sh
COPY --from=build /src/config /opt/cruise-control/config
COPY --from=build /src/cruise-control/build /opt/cruise-control/cruise-control/build
COPY --from=build /src/cruise-control-metrics-reporter/build/libs /opt/cruise-control/cruise-control-metrics-reporter/build/libs
RUN chmod +x /opt/cruise-control/kafka-cruise-control-start.sh \
  && mkdir -p /opt/cruise-control/logs
EXPOSE 9090
ENTRYPOINT ["/opt/cruise-control/kafka-cruise-control-start.sh"]
CMD ["/opt/cruise-control/config/cruisecontrol.properties"]
