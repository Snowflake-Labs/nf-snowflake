package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.snowflake.config.SnowflakeConfig
import nextflow.util.ServiceName
import nextflow.util.Duration
import org.pf4j.ExtensionPoint

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.sql.Statement
import java.sql.Connection
import java.sql.ResultSet

@Slf4j
@ServiceName('snowflake')
@CompileStatic
class SnowflakeExecutor extends Executor implements ExtensionPoint {
    SnowflakeConfig snowflakeConfig
    
    /**
     * A path where executable scripts from bin directory are copied
     */
    private Path remoteBinDir = null

    @Override
    protected TaskMonitor createTaskMonitor() {
        TaskPollingMonitor.create(session, config, name, Duration.of('10 sec'))
    }

    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        var registryMappings = buildRegistryMappings()

        return new SnowflakeTaskHandler(task, this, SnowflakeConnectionPool.getInstance(), registryMappings)
    }

    private Map<String, String> buildRegistryMappings() {
        Connection conn = SnowflakeConnectionPool.getInstance().getConnection();
        Statement stmt = conn.createStatement();
        final Map<String, String> registryMappings = new HashMap<>()

        String mappings = snowflakeConfig.registryMappings ?: ""

        // Skip processing if mappings is empty
        if (mappings == null || mappings.trim().isEmpty()) {
            return registryMappings
        }

        for (String mapping : mappings.split(",")) {
            String[] parts = mapping.split(":")
            
            // Skip if the mapping doesn't have both parts (original:replacement)
            if (parts.length < 2) {
                continue
            }
            
            String original = parts[0].trim()
            String replacement = parts[1].trim()

            ResultSet resultSet = stmt.executeQuery("show image repositories like '$replacement'")
            boolean hasNext = resultSet.next()

            if (hasNext) {
                String repoUrl = resultSet.getString("repository_url")
                registryMappings.put(original, repoUrl)
            }
        }

        return registryMappings
    }

    /**
     * @return The remote bin directory path
     */
    Path getRemoteBinDir() {
        remoteBinDir
    }

    /**
     * Copy local bin directory to Snowflake stage
     */
    protected void uploadBinDir() {
        if( session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir ) {
            // Use session run name for directory isolation
            final String runId = session.runName
            final Path workDirPath = session.workDir

            log.debug("Uploading bin directory to Snowflake stage: ${workDirPath.toUriString()}")

            // Upload to snowflake://stage/STAGE_NAME/<runId>/bin
            final Path targetDir = workDirPath.resolve(runId).resolve("bin")

            // Create target directory (implicit in Snowflake)
            Files.createDirectories(targetDir)

            // Copy all files from bin directory to stage
            Files.list(session.binDir).forEach { Path source ->
                final Path target = targetDir.resolve(source.getFileName())
                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
                log.debug("Uploaded bin file: ${source.getFileName()} -> ${target.toUriString()}")
            }

            remoteBinDir = targetDir
            log.debug("Bin directory uploaded to: ${targetDir.toUriString()}")
        }
    }

    /**
     * Initialise Snowflake executor.
     */
    @Override
    protected void register() {
        super.register()
        final Map configMap = session.config.navigate("snowflake") as Map
        snowflakeConfig = new SnowflakeConfig(configMap ?: [:])

        // Set config on connection pool for connection discovery
        SnowflakeConnectionPool.getInstance().setConfig(snowflakeConfig)

        // Validate that workDir uses snowflake:// scheme
        validateWorkDir()

        uploadBinDir()
    }

    /**
     * Validate that the work directory uses the snowflake:// scheme
     * @throws IllegalArgumentException if workDir doesn't use snowflake:// scheme
     */
    private void validateWorkDir() {
        final Path workDirPath = session.workDir
        if (!workDirPath) {
            throw new IllegalArgumentException(
                "Work directory is not configured. " +
                "For Snowflake executor, you must specify a work directory using the snowflake:// scheme, " +
                "e.g., workDir = 'snowflake://stage/MY_STAGE/work'"
            )
        }

        final String workDirStr = workDirPath.toUriString()
        if (!SnowflakeUri.isSnowflakeStageUri(workDirStr)) {
            throw new IllegalArgumentException(
                "Invalid work directory for Snowflake executor: ${workDirStr}\n" +
                "The work directory must use the snowflake:// scheme pointing to a Snowflake internal stage.\n" +
                "Example: workDir = 'snowflake://stage/MY_STAGE/work'\n" +
                "Current workDir: ${workDirStr}"
            )
        }

        log.debug("Using Snowflake stage as work directory: ${workDirStr}")
    }

    @Override
    boolean isContainerNative() {
        return true
    }

    @Override
    void shutdown() {
    }
}
