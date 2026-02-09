# nf-snowflake plugin

## Overview 
nf-snowflake is a [Nextflow](https://www.nextflow.io/docs/latest/overview.html) plugin which allows Nextflow pipeline to be run inside [Snowpark Container Service](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview).

This plugin requires both Nextflow main process and worker process being run as a container job inside Snowflake. Each process/task in Nextflow will be translated to a [Snowflake Job Service](https://docs.snowflake.com/en/sql-reference/sql/execute-job-service). The main process can be a job service or a long-running service. Intermediate result between different Nextflow processes will be shared via [stage mount](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/snowflake-stage-volume), so the same stage mount configuration needs to be applied to both main process container and worker process container.

## QuickStart

This quick start guide assumes you are familiar with both Nextflow and Snowpark Container Service.

1. Create a compute pool
```
CREATE COMPUTE POOL cp
MIN_NODES = 2
MAX_NODES = 2
INSTANCE_FAMILY = CPU_X64_M
auto_suspend_secs=3600
;
```
2. Create Snowflake Internal Stage for working directory
```
create or replace stage nxf_workdir encryption=(type = 'SNOWFLAKE_SSE');
```
4. Build the container image for each Nextflow [process](https://www.nextflow.io/docs/latest/process.html), upload the image to [Snowflake Image Registry](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-registry-repository) and update the each process's [container](https://www.nextflow.io/docs/latest/reference/process.html#process-container) field.
e.g.
```
process INDEX {
    tag "$transcriptome.simpleName"
    container '/db/schema/repo/image_name:1.0'

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
5. Add a snowflake profile to the nextflow.config file and enable nf-snowflake plugin e.g.
```
...
plugins {
  id 'nf-snowflake@1.0.0'
}
...
  snowflake {
    process.executor = 'snowflake'
    snowflake {
      computePool = 'CP'
    }
  }
...
```
6. Run nextflow pipeline with snowflake's Snowpark Container Service
```
nextflow run . -profile snowflake -work-dir snowflake://stage/NXF_WORKDIR/
```
