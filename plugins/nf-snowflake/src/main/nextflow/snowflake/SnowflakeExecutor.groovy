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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Statement
import java.sql.Connection
import java.sql.DriverManager

@Slf4j
@ServiceName('snowflake')
@CompileStatic
class SnowflakeExecutor extends Executor implements ExtensionPoint {

    static {
        Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")
    }

    private Connection connection

    String runtimeStageName

    Map snowflakeConfig

    @Override
    protected TaskMonitor createTaskMonitor() {
        TaskPollingMonitor.create(session, name, 1000, Duration.of('10 sec'))
    }

    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        Statement statement = connection.createStatement()
        return new SnowflakeTaskHandler(task, statement, this)
    }

    /**
     * Initialise Snowflake executor.
     */
    @Override
    protected void register() {
        super.register()
        snowflakeConfig = session.config.navigate("snowflake") as Map
        runtimeStageName = snowflakeConfig.get("runtimeStage")
        //runtimeStageName = String.format("nxf_%s_runtime", session.runName).toUpperCase()
        //runtimeStageName = String.format("nxf_runtime", session.runName).toUpperCase()
        initSFConnection()
    }

    private void initSFConnection() {
        final Properties properties = new Properties()
        properties.put("account", System.getenv("SNOWFLAKE_ACCOUNT"))
        properties.put("database", System.getenv("SNOWFLAKE_DATABASE"))
        properties.put("schema", System.getenv("SNOWFLAKE_SCHEMA"))
        properties.put("authenticator", "oauth")
        properties.put("token", readLoginToken())
        connection = DriverManager.getConnection(
                String.format("jdbc:snowflake://%s", System.getenv("SNOWFLAKE_HOST")),
                properties)
    }

    private static String readLoginToken() {
        return new String(Files.readAllBytes(Paths.get("/snowflake/session/token")), StandardCharsets.UTF_8);
    }

    @Override
    boolean isContainerNative() {
        return true
    }

    @Override
    void shutdown() {
        connection.close()
    }
}
