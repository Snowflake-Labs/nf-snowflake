package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import net.snowflake.client.jdbc.QueryStatusV2
import net.snowflake.client.jdbc.SnowflakeResultSet
import nextflow.exception.ProcessUnrecoverableException
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskConfig
import nextflow.processor.TaskStatus
import nextflow.snowflake.spec.Container
import nextflow.snowflake.spec.ResourceItems
import nextflow.snowflake.spec.Resources
import nextflow.snowflake.spec.SnowflakeJobServiceSpec
import nextflow.snowflake.spec.Spec
import nextflow.snowflake.spec.Volume
import nextflow.snowflake.spec.VolumeMount
import nextflow.util.Escape
import org.yaml.snakeyaml.DumperOptions
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.representer.Representer
import org.yaml.snakeyaml.introspector.BeanAccess
import org.yaml.snakeyaml.introspector.Property
import org.yaml.snakeyaml.nodes.MappingNode

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
    
    // Static YAML object for efficient reuse with custom representer
    private static final Yaml yaml = createYamlDumper()

    private static Yaml createYamlDumper() {
        DumperOptions dumperOptions = new DumperOptions()
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
        
        // Custom representer that skips any field when it's null
        Representer representer = new Representer(dumperOptions) {
            @Override
            protected MappingNode representJavaBean(Set<Property> properties, Object javaBean) {
                // Filter out any property when it's null
                Set<Property> filteredProperties = new LinkedHashSet<>()
                for (Property property : properties) {
                    try {
                        Object value = property.get(javaBean)
                        // Skip any property that has a null value
                        if (value == null) {
                            continue
                        }
                        filteredProperties.add(property)
                    } catch (Exception e) {
                        // If we can't get the value, include the property
                        filteredProperties.add(property)
                    }
                }
                return super.representJavaBean(filteredProperties, javaBean)
            }
        }
        
        return new Yaml(representer, dumperOptions)
    }

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
        TaskConfig taskCfg = this.task.getConfig()
        Container container = new Container()
        container.name = containerName
        container.image = task.container
        container.command = classicSubmitCli(task)

        final cpu = taskCfg.getCpus()
        final memory = taskCfg.getMemory()

        if (cpu || memory) {
            container.resources = new Resources()
            container.resources.requests = new ResourceItems()
            container.resources.requests.cpu = cpu ? cpu : null
            container.resources.requests.memory = memory ? memory.toMega() + "Mi" : null
        }

        final String mounts = executor.snowflakeConfig.get("stageMounts")
        StageMounts result = parseStageMounts(mounts)

        final String workDir = executor.getWorkDir().toUriString()
        final String workDirStage = executor.snowflakeConfig.get("workDirStage")
        result.addWorkDirMount(workDir, String.format("%s/%s/", workDirStage, executor.session.runName))

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

        return yaml.dump(root)
    }

    private static class StageMounts {
        final List<VolumeMount> volumeMounts;
        final List<Volume> volumes;

        StageMounts(){
            volumeMounts = new ArrayList<>()
            volumes = new ArrayList<>()
        }

        StageMounts(List<VolumeMount> volumeMounts, List<Volume> volumes){
            this.volumes = volumes
            this.volumeMounts = volumeMounts
        }

        void addWorkDirMount(String workDir, String workDirStage) {
            final String volumeName = "volume" + volumeMounts.size()
            volumeMounts.add(new VolumeMount(volumeName, workDir))
            volumes.add(new Volume(volumeName, "@"+workDirStage))
        }
    }
    
    private static StageMounts parseStageMounts(String input){
        if (input == null) {
            return new StageMounts()
        }

        final List<Volume> volumes = new ArrayList<>()
        final List<VolumeMount> volumeMounts = new ArrayList<>()
        String[] mounts = input.split(",")
        for (int i=0; i<mounts.length; i++) {
            String[] mountParts = mounts[i].split(":")
            if (mountParts.length != 2) {
                continue
            }

            final String volumeName = "volume" + i
            volumeMounts.add(new VolumeMount(volumeName, mountParts[1]))
            volumes.add(new Volume(volumeName, "@"+mountParts[0]))
        }

        return new StageMounts(volumeMounts, volumes)
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
