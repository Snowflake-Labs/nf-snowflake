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
import java.sql.SQLException
import java.sql.Statement

import net.snowflake.client.jdbc.SnowflakeStatement

@Slf4j
@CompileStatic
class SnowflakeTaskHandler extends TaskHandler {
    private Statement statement
    private ResultSet resultSet
    private SnowflakeExecutor executor
    private String jobServiceName
    private static final String containerName = 'main'

    SnowflakeTaskHandler(TaskRun taskRun, Statement statement, SnowflakeExecutor executor) {
        super(taskRun)
        this.statement = statement
        this.executor = executor
        this.jobServiceName = normalizeTaskName(executor.session.runName, task.getName())
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
        if (queryStatus.isSuccess()) {
            // execute job did not expose error code. Just use exit code 1 for all failure case
            task.exitStatus = 0
            task.stdout = tryGetStdout()
            this.status = TaskStatus.COMPLETED
            return true
        } else if (queryStatus.isAnError()) {
            task.exitStatus = 1
            task.stdout = tryGetStdout()
            task.stderr = queryStatus.errorMessage
            return true
        } else {
            return false
        }
    }

    private String tryGetStdout() {
        try {
            final Statement pollStmt = statement.getConnection().createStatement()
            final ResultSet resultSet = pollStmt.executeQuery(
                    String.format("select system\$GET_SERVICE_LOGS('%s', '0', '%s')", jobServiceName, containerName))
            boolean hasNext = resultSet.next()
            return hasNext ? resultSet.getString(1) : ""
        } catch (SQLException e) {
            return "Failed to read stdout: " + e.toString()
        }
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

        final String defaultComputePool = executor.snowflakeConfig.get("computePool")
        final String eai = executor.snowflakeConfig.getOrDefault("externalAccessIntegrations", "")

        String executeSql = String.format("""
execute job service
in compute pool %s
name = %s
external_access_integrations=(%s)
from specification
\$\$
%s
\$\$
""", defaultComputePool, jobServiceName, eai, spec)

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
        container.name = containerName
        container.image = task.container
        container.command = classicSubmitCli(task)

        final String mounts = executor.snowflakeConfig.get("stageMounts")
        StageMountsParseResult result = parseStageMounts(mounts)

        if (!result.volumeMounts.empty){
            container.volumeMounts = result.volumeMounts
        }

        Spec spec = new Spec()
        spec.containers = Collections.singletonList(container)

        if (!result.volumes.empty){
            spec.volumes = result.volumes
        }

        SnowflakeJobServiceSpec root = new SnowflakeJobServiceSpec()
        root.spec = spec

        DumperOptions dumperOptions = new DumperOptions()
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
        Yaml yaml = new Yaml(dumperOptions)
        return yaml.dump(root)
    }

    private static class StageMountsParseResult {
        final List<VolumeMount> volumeMounts;
        final List<Volume> volumes;

        StageMountsParseResult(){
            volumeMounts = Collections.emptyList()
            volumes = Collections.emptyList()
        }

        StageMountsParseResult(List<VolumeMount> volumeMounts, List<Volume> volumes){
            this.volumes = volumes
            this.volumeMounts = volumeMounts
        }
    }
    
    private static StageMountsParseResult parseStageMounts(String input){
        if (input == null) {
            return new StageMountsParseResult()
        }

        final List<Volume> volumes = new ArrayList<>()
        final List<VolumeMount> volumeMounts = new ArrayList<>()
        String[] mounts = input.split(",")
        for (int i=0; i<mounts.length; i++) {
            String[] mountParts = mounts[i].split(":")
            if (mountParts.length != 2) {
                continue
            }

            if (!mountParts[0].startsWith('@')){
                continue
            }

            final String volumeName = "volume" + i
            volumeMounts.add(new VolumeMount(volumeName, mountParts[1]))
            volumes.add(new Volume(volumeName, mountParts[0]))
        }

        return new StageMountsParseResult(volumeMounts, volumes)
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
