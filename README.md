# nf-snowflake plugin

## Overview 
nf-snowflake is a [Nextflow](https://www.nextflow.io/docs/latest/overview.html) plugin which allows Nextflow pipeline to be run inside [Snowpark Container Service](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview).

This plugin requires both Nextflow main process and worker process being run as a container job inside Snowflake. Each process/task in Nextflow will be translated to a [Snowflake Job Service](https://docs.snowflake.com/en/sql-reference/sql/execute-job-service). The main process can be a job service or a long-running service. Intermediate result between different Nextflow processes will be shared via [stage mount](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/snowflake-stage-volume), so the same stage mount configuration needs to be applied to both main process container and worker process container.

## QuickStart

This quick start guide assumes you are familiar with both Nextflow and Snowpark Container Service.

1. Create Snowflake Resources
```sql
-- Database, schema, image repository
CREATE OR REPLACE DATABASE TUTORIAL_DB;
USE DATABASE TUTORIAL_DB;
CREATE OR REPLACE SCHEMA DATA_SCHEMA;
CREATE IMAGE REPOSITORY IF NOT EXISTS TUTORIAL_REPOSITORY;
```

2. Create a compute pool
```sql
CREATE COMPUTE POOL NEXTFLOW_CP
  MIN_NODES = 2
  MAX_NODES = 2
  INSTANCE_FAMILY = CPU_X64_M
  auto_suspend_secs=3600
  ;
```

3. Create External Access Integration to allow cloud blob storage service access. You can follow the steps [here](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/additional-considerations-services-jobs#network-egress).
```sql
CREATE NETWORK RULE nf_nextflow_rule
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = (
    'www.nextflow.io:443',
    'github.com:443',
    'raw.githubusercontent.com:443',
    'objects.githubusercontent.com:443'
  );

CREATE EXTERNAL ACCESS INTEGRATION nf_nextflow_eai
  ALLOWED_NETWORK_RULES = (nf_nextflow_rule)
  ENABLED = TRUE;
```

4. Create Snowflake Internal Stage for working directory, input directory and publish directory
```
CREATE OR REPLACE STAGE NXF_INPUT   ENCRYPTION=(TYPE='SNOWFLAKE_SSE');
CREATE OR REPLACE STAGE NXF_RESULTS ENCRYPTION=(TYPE='SNOWFLAKE_SSE');
CREATE OR REPLACE STAGE NXF_WORKDIR ENCRYPTION=(TYPE='SNOWFLAKE_SSE');
```

5. Build the container image for each Nextflow [process](https://www.nextflow.io/docs/latest/process.html), upload the image to [Snowflake Image Registry](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-registry-repository) and update the each process's [container](https://www.nextflow.io/docs/latest/reference/process.html#process-container) field.
e.g.
`main.tf`
```groovy
process INDEX {
    tag "$transcriptome.simpleName"
    container '/TUTORIAL_DB/DATA_SCHEMA/TUTORIAL_REPOSITORY/hello-worker:1.0'

    input:
    path transcriptome

    output:
    path 'index'

    script:
    """
    salmon index --threads $task.cpus -t $transcriptome -i index
    """
}
```

6. Add a snowflake profile to the nextflow.config file and enable nf-snowflake plugin e.g.
`nextflow.config`
```groovy
plugins { id 'nf-snowflake@0.6.3' }

profiles {
  snowflake {
    process.executor      = 'snowflake'
    snowflake.computePool = 'NEXTFLOW_CP'
    snowflake.stageMounts = 'NXF_RESULTS:/mnt/output,NXF_INPUT:/mnt/input'
    snowflake.externalAccessIntegrations='nf_nextflow_eai'
    snowflake.workDirStage= 'NXF_WORKDIR'
  }
}
```
7. Build the container image for Nextflow main process, you will need to include nf-snowflake plugin. You might consider include your Nextflow pipeline code as well. Here is a sample Dockerfile(tag example: `nxf-main:latest`):
```dockerfile
FROM nextflow/nextflow:24.04.1

RUN nextflow plugin install nf-snowflake
COPY . /rnaseq-nf
WORKDIR /rnaseq-nf
```


8. Create worker image(Tag example: `hello-worker:latest`)
```dockerfile
FROM mambaorg/micromamba
RUN micromamba install -y -n base -c bioconda -c conda-forge \
      salmon fastqc multiqc python=3.11 && \
    micromamba clean -a -y
ENV PATH="$MAMBA_ROOT_PREFIX/bin:$PATH"
USER root
```

9. Build & push (replace `REG`):
```bash
REG="<ORG>-<ACCOUNT>.registry.snowflakecomputing.com"

# Main
docker build -f docker/Dockerfile.main \
  -t $REG/TUTORIAL_DB/DATA_SCHEMA/TUTORIAL_REPOSITORY/nxf-main:latest .
docker push $REG/.../nxf-main:latest

# Worker
docker build -f docker/Dockerfile.worker \
  -t $REG/TUTORIAL_DB/DATA_SCHEMA/TUTORIAL_REPOSITORY/hello-worker:latest .
docker push $REG/.../hello-worker:latest
```

10. Start the container service to trigger the pipeline:
```sql
EXECUTE JOB SERVICE
  IN COMPUTE POOL NEXTFLOW_CP
  NAME = HELLO_NXF
  EXTERNAL_ACCESS_INTEGRATIONS = (nf_nextflow_eai)
  FROM SPECIFICATION $$
  spec:
      container:
      - name: main
        image: /TUTORIAL_DB/DATA_SCHEMA/TUTORIAL_REPOSITORY/nxf-main:latest
        volumeMounts:
        - name: input
          mountPath: /mnt/input
        - name: output
          mountPath: /mnt/output
        - name: workdir
          mountPath: /mnt/workdir
        command:
        - nextflow
        - run
        - .
        - -profile
        - snowflake
        - -work-dir
        - /mnt/workdir
        - -resume
      volumes:
      - name: input
        source: "@NXF_INPUT"
      - name: output
        source: "@NXF_RESULTS"
      - name: workdir
        source: "@NXF_WORKDIR"
  $$;
```

Check logs
```sql
SELECT SYSTEM$GET_SERVICE_LOGS('HELLO_NXF', 0, 'main');
```
