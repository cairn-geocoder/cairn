# Minimal Photon 1.x serve container. The on-disk OpenSearch index
# is built once on the host (see ../README.md → photon-build.sh)
# and bind-mounted at runtime so the image stays tiny + reusable.
#
# We tried building the index inside the image but the
# `import` step crashed in the buildkit context — runs cleanly on
# the host. Pre-built index path works for the benchmark since we
# only care about query-time numbers.

FROM eclipse-temurin:21-jre
ARG PHOTON_VERSION=1.1.0
WORKDIR /photon
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSLO "https://github.com/komoot/photon/releases/download/${PHOTON_VERSION}/photon-${PHOTON_VERSION}.jar" \
    && mv "photon-${PHOTON_VERSION}.jar" photon.jar
EXPOSE 2322
ENTRYPOINT ["java", "-jar", "photon.jar", "serve", "-listen-ip", "0.0.0.0", "-listen-port", "2322", "-data-dir", "/photon/data"]
