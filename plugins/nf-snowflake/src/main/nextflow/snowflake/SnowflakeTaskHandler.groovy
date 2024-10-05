package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import net.snowflake.client.jdbc.QueryStatusV2
import net.snowflake.client.jdbc.SnowflakeResultSet
import nextflow.exception.ProcessUnrecoverableException
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.snowflake.spec.Container
import nextflow.snowflake.spec.SnowflakeJobServiceSpec
import nextflow.snowflake.spec.Spec
import nextflow.snowflake.spec.Volume
import nextflow.snowflake.spec.VolumeMount
import nextflow.util.Escape
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml

import java.sql.ResultSet
import java.sql.Statement

import net.snowflake.client.jdbc.SnowflakeStatement

@Slf4j
@CompileStatic
class SnowflakeTaskHandler extends TaskHandler {
    private Statement statement
    private ResultSet resultSet
    private SnowflakeExecutor executor

    SnowflakeTaskHandler(TaskRun taskRun, Statement statement, SnowflakeExecutor executor) {
        super(taskRun)
        this.statement = statement
        this.executor = executor
        validateConfiguration()
    }


    @Override
    boolean checkIfRunning() {
        QueryStatusV2 queryStatus = resultSet.unwrap(SnowflakeResultSet).getStatusV2()
        boolean isRunningOrCompleted = queryStatus.isStillRunning() || queryStatus.isSuccess() || queryStatus.isAnError()
        if (isRunningOrCompleted) {
            this.status = TaskStatus.RUNNING
        }
        return isRunningOrCompleted
    }

    @Override
    boolean checkIfCompleted() {
        if( !isRunning() )
            return false

        QueryStatusV2 queryStatus = resultSet.unwrap(SnowflakeResultSet).getStatusV2()
        boolean done = queryStatus.isSuccess() || queryStatus.isAnError()
        if (done) {
            // execute job did not expose error code. Just use exit code 1 for all failure case
            task.exitStatus = queryStatus.isSuccess() ? 0 : 1
            task.stdout = ""
            this.status = TaskStatus.COMPLETED
        }
        return done
    }

    @Override
    void kill(){
        statement.cancel()
    }

    @Override
    void submit(){
        // create bash wrapper script
        final SnowflakeWrapperBuilder builder = new SnowflakeWrapperBuilder(task)
        builder.build()

        final String spec = buildJobServiceSpec()
        final String jobServiceName = normalizeTaskName(executor.session.runName, task.getName())
        final String defaultComputePool = executor.snowflakeConfig.get("computePool")

        final String executeSql = String.format("""
execute job service
in compute pool %s
name = %s
from specification
\$\$
%s
\$\$
""", defaultComputePool, jobServiceName, spec)

        resultSet = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(executeSql)
        this.status = TaskStatus.SUBMITTED
    }

    private void validateConfiguration() {
        if (!task.container) {
            throw new ProcessUnrecoverableException("No container image specified for process $task.name -- Either specify the container to use in the process definition or with 'process.container' value in your config")
        }

        //TODO validate compute pool is specified
    }

    private String buildJobServiceSpec() {
        Container container = new Container()
        container.name = 'main'
        container.image = task.container
        container.command = classicSubmitCli(task)
        container.volumeMounts = new ArrayList<>(Arrays.asList(
                new VolumeMount("nxf-runtime", executor.session.workDir.toString())
        ))

        Spec spec = new Spec()
        spec.containers = Collections.singletonList(container)
        spec.volumes = new ArrayList<>(Arrays.asList(
                new Volume("nxf-runtime", String.format("@%s", executor.runtimeStageName))
        ))

        SnowflakeJobServiceSpec root = new SnowflakeJobServiceSpec()
        root.spec = spec

        DumperOptions dumperOptions = new DumperOptions()
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
        Yaml yaml = new Yaml(dumperOptions)
        return yaml.dump(root)
    }

    private static String normalizeTaskName(String sessionRunName, String taskName) {
        String jobName = (sessionRunName + "_" + taskName).replaceAll("[^A-Za-z0-9]", "_")
        return jobName.replaceAll("^_+", "").replaceAll("_+\$", "")
    }

    private static List<String> classicSubmitCli(TaskRun task) {
        final result = new ArrayList(BashWrapperBuilder.BASH)
        result.add("${Escape.path(task.workDir)}/${TaskRun.CMD_RUN}".toString())
        return result
    }

}
