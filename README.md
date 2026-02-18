# nf-snowflake plugin

## Overview
nf-snowflake is a [Nextflow](https://www.nextflow.io/docs/latest/overview.html) plugin that enables Nextflow pipelines to run inside [Snowpark Container Service](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview).

Each Nextflow task is translated to a [Snowflake Job Service](https://docs.snowflake.com/en/sql-reference/sql/execute-job-service) and executed as an SPCS job. The Nextflow main/driver program can run in two modes:

1. **Locally** - Running on your local machine or CI/CD environment, connecting to Snowflake via JDBC
2. **Inside SPCS** - Running as a separate SPCS job within Snowpark Container Services

These two execution modes correspond to the two authentication methods supported by the plugin. When the main/driver program runs inside an SPCS job, Snowflake automatically injects the required environment variables (such as `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_HOST`, etc.) and the session token file (`/snowflake/session/token`). The plugin automatically discovers and uses these credentials for authentication.

Intermediate results between different Nextflow processes are shared via [Snowflake stages](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage), which must be configured as the working directory.

## Prerequisites

Before using this plugin, you should have:

- **Nextflow** (version 23.04.0 or later)
- **Snowflake account** with access to:
  - Snowpark Container Services (Compute Pools/Image Registries)
  - Internal stages
- **Familiarity with**:
  - Nextflow pipelines and configuration
  - Docker/container images
  - Snowflake authentication methods

## Authentication

The plugin supports two authentication methods, corresponding to the two execution modes for the main/driver program:

### 1. Session Token Authentication (Main/Driver Running Inside SPCS)

When the Nextflow main/driver program runs inside an SPCS job, Snowflake automatically injects the session token file at `/snowflake/session/token` and the following environment variables:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_HOST`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_WAREHOUSE` (optional)

The plugin automatically discovers and uses these credentials for authentication. No additional configuration is required.

### 2. Connections.toml Authentication (Main/Driver Running Locally)

When the Nextflow main/driver program runs locally (on your machine or in CI/CD), the plugin uses the Snowflake [connections.toml](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure#connecting-using-the-connections-toml-file) configuration file for authentication.

**File Locations** (searched in order):
1. `~/.snowflake/connections.toml` (if directory exists)
2. Location specified in `SNOWFLAKE_HOME` environment variable
3. OS-specific defaults:
   - Linux: `~/.config/snowflake/connections.toml`
   - macOS: `~/Library/Application Support/snowflake/connections.toml`
   - Windows: `%USERPROFILE%\AppData\Local\snowflake\connections.toml`

**Example connections.toml:**
```toml
[default]
account = "myaccount"
user = "myuser"
password = "mypassword"
database = "mydb"
schema = "myschema"
warehouse = "mywh"

[production]
account = "prodaccount"
authenticator = "externalbrowser"
database = "proddb"
schema = "public"
```

**Specify a connection in nextflow.config:**
```groovy
snowflake {
    connectionName = 'production'
    computePool = 'MY_COMPUTE_POOL'
}
```

If no `connectionName` is specified, the plugin will use:
1. Connection name from `SNOWFLAKE_DEFAULT_CONNECTION_NAME` environment variable
2. The `default` connection from connections.toml

## Configuration Reference

All plugin configurations are defined under the `snowflake` scope in your `nextflow.config`:

### computePool

The name of the Snowflake compute pool to use for executing jobs.

```groovy
snowflake {
    computePool = 'MY_COMPUTE_POOL'
}
```

### registryMappings

Docker registry mappings for container images. Snowflake does not support pulling images directly from arbitrary external registries. Instead, you must first replicate container images from external registries (such as Docker Hub, GitHub Container Registry, etc.) to Snowflake image repositories.

The `registryMappings` configuration allows you to automatically replace external registry hostnames with Snowflake image repository names in your pipeline's container specifications.

**Format:** Comma-separated list of mappings in the form `external_registry:snowflake_repository`

```groovy
snowflake {
    registryMappings = 'docker.io:my_registry,ghcr.io:github_registry'
}
```

**How it works:**
1. First, replicate images to your Snowflake image repository:
   ```bash
   docker pull docker.io/alpine:latest
   docker tag docker.io/alpine:latest <snowflake_repo_url>/alpine:latest
   docker push <snowflake_repo_url>/alpine:latest
   ```

2. Then, when your process uses `container 'docker.io/alpine:latest'`, the plugin automatically replaces `docker.io` with your Snowflake image repository URL, resulting in the correct Snowflake image reference.

### connectionName

The name of the connection to use from the connections.toml file. When specified, the JDBC driver will use the connection configuration defined under this name.

```groovy
snowflake {
    connectionName = 'production'
}
```

**Note:** This is only used when the session token file is not available (i.e., when running outside Snowpark Container Services).

## Quick Start

This guide assumes you are familiar with both Nextflow and Snowpark Container Services.

### 1. Create a Compute Pool

```sql
CREATE COMPUTE POOL my_compute_pool
MIN_NODES = 2
MAX_NODES = 5
INSTANCE_FAMILY = CPU_X64_M
AUTO_SUSPEND_SECS = 3600;
```

### 2. Create a Snowflake Internal Stage for Working Directory

```sql
CREATE OR REPLACE STAGE nxf_workdir
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
```

### 3. Set Up Image Repository

```sql
CREATE IMAGE REPOSITORY IF NOT EXISTS my_images;
```

### 4. Build and Upload Container Images

Build the container image for each Nextflow [process](https://www.nextflow.io/docs/latest/process.html), upload the image to [Snowflake Image Registry](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-registry-repository), and update each process's [container](https://www.nextflow.io/docs/latest/reference/process.html#process-container) field.

**Example process definition:**
```groovy
process INDEX {
    tag "$transcriptome.simpleName"
    container '/mydb/myschema/my_images/salmon:1.10.0'

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

### 5. Configure Nextflow

Add a Snowflake profile to your `nextflow.config` file and enable the nf-snowflake plugin:

```groovy
plugins {
    id 'nf-snowflake@1.0.0'
}

profiles {
    snowflake {
        process.executor = 'snowflake'

        snowflake {
            computePool = 'my_compute_pool'
            registryMappings = 'docker.io:my_images'
        }
    }
}
```

### 6. Run Your Pipeline

Execute the Nextflow pipeline with the Snowflake profile:

```bash
nextflow run . -profile snowflake -work-dir snowflake://stage/nxf_workdir/
```

## Snowflake Filesystem and Working Directory

### Snowflake Stage URI

The plugin uses a custom URI scheme to access Snowflake internal stages:

```
snowflake://stage/<stage_name>/<path>
```

**Components:**
- `snowflake://` - URI scheme identifier
- `stage/` - Literal prefix indicating a Snowflake stage
- `<stage_name>` - The name of your Snowflake internal stage
- `<path>` - Optional path within the stage

**Examples:**
```groovy
// Access root of a stage
workDir = 'snowflake://stage/my_stage/'

// Access a subdirectory within a stage
workDir = 'snowflake://stage/my_stage/workflows/pipeline1/'
```

### Working Directory Requirement

**IMPORTANT:** The Nextflow working directory (`workDir`) **must** be a Snowflake stage using the `snowflake://` URI scheme. This is a strict requirement for the plugin to function correctly.

The working directory is used to:
- Store intermediate task results
- Share data between pipeline processes
- Store task execution metadata and logs

**Correct configuration:**
```groovy
profiles {
    snowflake {
        process.executor = 'snowflake'
        workDir = 'snowflake://stage/nxf_workdir/'  // ✓ Valid

        snowflake {
            computePool = 'my_compute_pool'
        }
    }
}
```

**Or specify on the command line:**
```bash
nextflow run . -profile snowflake -work-dir snowflake://stage/nxf_workdir/
```

**Invalid configurations:**
```groovy
workDir = 's3://my-bucket/work/'              // ✗ Invalid - not a Snowflake stage
workDir = '/local/path/work/'                 // ✗ Invalid - local filesystem
workDir = 'snowflake://my_stage/work/'        // ✗ Invalid - missing 'stage/' prefix
```

### Stage Setup

Before running your pipeline, ensure your stage is properly configured:

```sql
-- Create an internal stage with encryption
CREATE OR REPLACE STAGE my_workdir
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Verify stage exists
SHOW STAGES LIKE 'my_workdir';

-- Optional: Test stage access
LIST @my_workdir;
```

## Additional Resources

- [Nextflow Documentation](https://www.nextflow.io/docs/latest/index.html)
- [Snowpark Container Services Documentation](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview)
- [Snowflake JDBC Configuration](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure)
- [Nextflow Plugin Development](https://www.nextflow.io/docs/latest/plugins.html)
