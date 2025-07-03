FROM nextflow/nextflow:24.04.1

# Define version argument with default value
ARG PLUGIN_VERSION=0.6.0

# Create plugins directory
RUN mkdir -p /.nextflow/plugins

# Copy the locally built plugin (recursively copy the entire directory)
COPY build/plugins/nf-snowflake-${PLUGIN_VERSION}/ /.nextflow/plugins/nf-snowflake-${PLUGIN_VERSION}/

RUN yum install -y git tar

RUN curl -O https://bootstrap.pypa.io/get-pip.py && \
    python3 get-pip.py && \
    rm get-pip.py && \
    pip install websockets

COPY docker/nf-snowflake/pty_server.py /app/pty_server.py

# Set working directory
WORKDIR /workspace
