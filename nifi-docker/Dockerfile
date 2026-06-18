FROM apache/nifi:2.3.0
USER root
# python3.12 is not in Debian 12 repos. Install via uv's python-build-standalone,
# then symlink so NiFi's nifi.python.command=python3 resolves to 3.12,
# matching the python3.12 on the production NiFi host.
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl libopenjp2-7 \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh \
    && uv python install 3.12 \
    && ln -sf "$(uv python find 3.12)" /usr/bin/python3
USER nifi
