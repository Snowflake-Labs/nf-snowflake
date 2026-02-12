FROM nextflow/nextflow:25.04.6

# Define version argument with default value
ARG PLUGIN_VERSION=1.1.0

# Install dependencies
RUN yum install -y unzip git tar

# Install Python dependencies
RUN curl -O https://bootstrap.pypa.io/get-pip.py && \
    python3 get-pip.py && \
    rm get-pip.py && \
    pip install aiohttp

COPY docker/nf-snowflake/pty_server.py /app/pty_server.py

# Create plugins directory
RUN mkdir -p /.nextflow/plugins

# Copy the plugin distribution zip and extract it
COPY build/distributions/nf-snowflake-${PLUGIN_VERSION}.zip /tmp/
RUN unzip /tmp/nf-snowflake-${PLUGIN_VERSION}.zip -d /.nextflow/plugins/nf-snowflake-${PLUGIN_VERSION}/ && \
    rm /tmp/nf-snowflake-${PLUGIN_VERSION}.zip

# Set working directory
WORKDIR /workspace
