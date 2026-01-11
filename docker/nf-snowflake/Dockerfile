FROM nextflow/nextflow:25.04.6

# Define version argument with default value
ARG PLUGIN_VERSION=0.9.0

# Create plugins directory
RUN mkdir -p /.nextflow/plugins

# Copy the locally built plugin (recursively copy the entire directory)
COPY build/plugins/nf-snowflake-${PLUGIN_VERSION}/ /.nextflow/plugins/nf-snowflake-${PLUGIN_VERSION}/

RUN yum install -y git tar

RUN curl -O https://bootstrap.pypa.io/get-pip.py && \
    python3 get-pip.py && \
    rm get-pip.py && \
    pip install aiohttp

COPY docker/nf-snowflake/pty_server.py /app/pty_server.py
COPY docker/nf-snowflake/websocket_log_server.py /app/websocket_log_server.py
COPY docker/nf-snowflake/nextflow_runner.py /app/nextflow_runner.py

# Set working directory
WORKDIR /workspace
