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
        connection = SnowflakeConnectionPool.createConn()
    }

    @Override
    boolean isContainerNative() {
        return true
    }

    @Override
    void shutdown() {
        // Return connection to the pool instead of closing directly
        if (connection != null) {
            SnowflakeConnectionPool.returnConn(connection)
        }
    }
}
