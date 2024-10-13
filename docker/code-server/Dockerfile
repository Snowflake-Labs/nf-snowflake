FROM linuxserver/code-server

USER root
RUN mkdir -p /usr/lib/jvm && \
    curl -O https://download.oracle.com/java/21/archive/jdk-21.0.3_linux-x64_bin.tar.gz && \
    tar xvzf jdk-21.0.3_linux-x64_bin.tar.gz -C /usr/lib/jvm

ENV JAVA_HOME=/usr/lib/jvm/jdk-21.0.3
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN curl -s https://get.nextflow.io | bash && chmod 0755 nextflow && mv nextflow /usr/bin
RUN /app/code-server/bin/code-server --install-extension nextflow.nextflow --extensions-dir /config/extensions

RUN mkdir -p /config/.nextflow/plugins
COPY build/plugins/ /config/.nextflow/plugins/
