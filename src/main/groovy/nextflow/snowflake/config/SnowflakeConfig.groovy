package nextflow.snowflake.config

import groovy.transform.CompileStatic
import groovy.transform.ToString
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description

/**
 * Configuration scope for Snowflake executor plugin.
 *
 * This class defines all configuration options available under the 'snowflake' scope
 * in Nextflow configuration files.
 *
 * @author Hongxin Yu <hongxin.yu@snowflake.com>
 */
@ScopeName('snowflake')
@Description('''
    The `snowflake` scope allows you to configure the Snowflake executor plugin.

    Example configuration:
    ```
    snowflake {
        computePool = 'MY_COMPUTE_POOL'
        registryMappings = 'docker.io:my_registry,ghcr.io:github_registry'
    }
    ```
''')
@CompileStatic
@ToString(includeNames = true, includePackage = false)
class SnowflakeConfig implements ConfigScope {

    @ConfigOption
    @Description('''
        The name of the Snowflake compute pool to use for executing jobs.
        This can also be configured via the `computePool` environment variable.

        Example: `computePool = 'MY_COMPUTE_POOL'`
    ''')
    String computePool

    @ConfigOption
    @Description('''
        Docker registry mappings for container images. This allows mapping external
        Docker registries to Snowflake image repositories.

        Format: Comma-separated list of mappings in the form `original:replacement`

        Example: `registryMappings = 'docker.io:my_registry,ghcr.io:github_registry'`
    ''')
    String registryMappings

    @ConfigOption
    @Description('''
        The name of the connection to use from the connections.toml file.
        When specified, the JDBC driver will use the connection configuration
        defined under this name in the connections.toml file.

        Example: `connectionName = 'aws-oauth-file'`
    ''')
    String connectionName

    /**
     * No-argument constructor required for configuration validation support.
     */
    SnowflakeConfig() {
    }

    /**
     * Constructor accepting a Map for initialization from Nextflow configuration.
     *
     * @param opts Configuration options map
     */
    SnowflakeConfig(Map opts) {
        if (opts == null) {
            return
        }

        this.computePool = opts.computePool as String
        this.registryMappings = opts.registryMappings as String
        this.connectionName = opts.connectionName as String
    }
}
