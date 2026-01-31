package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.Executor
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
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
    Map snowflakeConfig
    
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

        String mappings = snowflakeConfig.get("registryMappings", "")

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
     * Copy local bin directory to remote mounted workdir
     */
    protected void uploadBinDir() {
        if( session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir ) {
            // Use session run name for directory isolation
            final String runId = session.runName
            final Path targetDir = Paths.get("/mnt/workdir", runId, "bin")
            
            // Create target directory
            Files.createDirectories(targetDir)
            
            // Copy all files from bin directory using standard file I/O
            Files.list(session.binDir).forEach { Path source ->
                final Path target = targetDir.resolve(source.getFileName())
                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
            }
            
            remoteBinDir = targetDir
        }
    }

    /**
     * Initialise Snowflake executor.
     */
    @Override
    protected void register() {
        super.register()
        snowflakeConfig = session.config.navigate("snowflake") as Map
        uploadBinDir()
    }

    @Override
    boolean isContainerNative() {
        return true
    }

    @Override
    void shutdown() {
    }
}
