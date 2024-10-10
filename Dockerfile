FROM nextflow/nextflow:24.04.1

COPY build/plugins/nf-snowflake-0.5.0/ /.nextflow/plugins/nf-snowflake-0.5.0/

RUN VER=$( curl --silent "https://api.github.com/repos/tsl0922/ttyd/releases/latest"| grep '"tag_name"'|sed -E 's/.*"([^"]+)".*/\1/') \
    && curl -LO https://github.com/tsl0922/ttyd/releases/download/$VER/ttyd.x86_64 \
    && mv ttyd.* /usr/local/bin/ttyd \
    && chmod +x /usr/local/bin/ttyd

WORKDIR /
