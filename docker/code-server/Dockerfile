FROM linuxserver/code-server

USER root
RUN mkdir -p /usr/lib/jvm && \
    curl -O https://download.java.net/java/GA/jdk17/0d483333a00540d886896bac774ff48b/35/GPL/openjdk-17_linux-x64_bin.tar.gz && \
    tar xvzf openjdk-17_linux-x64_bin.tar.gz -C /usr/lib/jvm

ENV JAVA_HOME=/usr/lib/jvm/jdk-17
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN curl -s https://get.nextflow.io | bash && chmod 0755 nextflow && mv nextflow /usr/bin
RUN mkdir -p /config/.nextflow/plugins
COPY build/plugins/nf-snowflake-0.5.0/ /config/.nextflow/plugins/nf-snowflake-0.5.0/
