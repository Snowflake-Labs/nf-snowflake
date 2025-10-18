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

import java.sql.Statement
import java.sql.Connection
import java.sql.ResultSet

@Slf4j
@ServiceName('snowflake')
@CompileStatic
class SnowflakeExecutor extends Executor implements ExtensionPoint {
    Map snowflakeConfig

    @Override
    protected TaskMonitor createTaskMonitor() {
        TaskPollingMonitor.create(session, name, 1000, Duration.of('10 sec'))
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

        for (String mapping : mappings.split(",")) {
            String[] parts = mapping.split(":")
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
     * Initialise Snowflake executor.
     */
    @Override
    protected void register() {
        super.register()
        snowflakeConfig = session.config.navigate("snowflake") as Map
    }

    @Override
    boolean isContainerNative() {
        return true
    }

    @Override
    void shutdown() {
    }
}
