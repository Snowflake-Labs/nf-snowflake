FROM nextflow/nextflow:24.04.1

# Define version argument with default value
ARG PLUGIN_VERSION=0.6.0

# Create plugins directory
RUN mkdir -p /.nextflow/plugins

# Copy the locally built plugin (recursively copy the entire directory)
COPY build/plugins/nf-snowflake-${PLUGIN_VERSION}/ /.nextflow/plugins/nf-snowflake-${PLUGIN_VERSION}/

RUN yum install -y git tar

# Set working directory
WORKDIR /workspace
