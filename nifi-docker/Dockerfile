FROM apache/nifi:2.3.0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libopenjp2-7 && rm -rf /var/lib/apt/lists/*
USER nifi
