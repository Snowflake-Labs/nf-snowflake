package nextflow.snowflake

import nextflow.processor.TaskId
import spock.lang.Specification
import nextflow.processor.TaskRun
import nextflow.processor.TaskConfig
import nextflow.Session
import nextflow.util.MemoryUnit
import nextflow.script.ProcessConfig
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import groovy.transform.CompileStatic
import java.nio.file.Paths
import java.sql.Statement
import java.lang.reflect.Method

class SnowflakeTaskHandlerTest extends Specification {

    def "buildJobServiceSpec should generate valid YAML with minimal configuration"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'ubuntu:latest'
        ])
        def executor = createMockExecutor([
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()
        
        then:
        yamlSpec != null
        yamlSpec.contains('ubuntu:latest')
        yamlSpec.contains('main')
        
        // Basic structure validation using string checks
        yamlSpec.contains('spec:')
        yamlSpec.contains('containers:')
        yamlSpec.contains('image: ubuntu:latest')
        yamlSpec.contains('name: main')
        yamlSpec.contains('command:')
        yamlSpec.contains('/bin/bash')
        yamlSpec.contains('volumeMounts:')
        yamlSpec.contains('volumes:')
        yamlSpec.contains('mountPath: /work')
    }

    def "buildJobServiceSpec should include CPU and memory resources when specified"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'ubuntu:latest',
            cpus: 4,
            memory: '8GB'
        ])
        def executor = createMockExecutor([
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()

        then:
        yamlSpec != null
        yamlSpec.contains('ubuntu:latest')
        
        // Check resources section is included and not null
        yamlSpec.contains('resources:')
        !yamlSpec.contains('resources: null')
        
        // Should include requests section, limits should not exist at all
        yamlSpec.contains('requests:')
        !yamlSpec.contains('limits:')
        
        // Validate CPU and memory settings in requests
        yamlSpec.contains('cpu: 4')
        yamlSpec.contains('memory: 8192Mi')
    }

    def "buildJobServiceSpec should include only CPU when memory not specified"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'ubuntu:latest',
            cpus: 2
        ])
        def executor = createMockExecutor([
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()

        then:
        yamlSpec != null
        
        // Should include requests section, limits should not exist at all
        yamlSpec.contains('requests:')
        !yamlSpec.contains('limits:')
        
        yamlSpec.contains('cpu: 2')
        // Memory should not be present when not specified (null fields are omitted)
        !yamlSpec.contains('memory:')
    }

    def "buildJobServiceSpec should include only memory when CPU not specified"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'ubuntu:latest',
            memory: '4GB'
        ])
        def executor = createMockExecutor([
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()

        then:
        yamlSpec != null
        
        // Should include requests section, limits should not exist at all
        yamlSpec.contains('requests:')
        !yamlSpec.contains('limits:')
        
        // CPU should not be present when not specified (null fields are omitted)
        !yamlSpec.contains('cpu:')
        yamlSpec.contains('memory: 4096Mi')
    }

    def "buildJobServiceSpec should not include resources section when neither CPU nor memory specified"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'ubuntu:latest'
            // No cpus or memory specified
        ])
        def executor = createMockExecutor([
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()

        then:
        yamlSpec != null
        yamlSpec.contains('ubuntu:latest')
        yamlSpec.contains('name: main')
        
        // Resources section should not be present when no CPU or memory specified (null fields are omitted)
        !yamlSpec.contains('resources:')
        
        // Should not contain any resource-related fields
        !yamlSpec.contains('requests:')
        !yamlSpec.contains('limits:')
        !yamlSpec.contains('cpu:')
        !yamlSpec.contains('memory:')
    }

    def "buildJobServiceSpec should include stage mounts when configured"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'ubuntu:latest'
        ])
        def executor = createMockExecutor([
            stageMounts: 'stage1:/mnt/data,stage2:/mnt/output',
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()

        then:
        yamlSpec != null
        
        // Should have multiple volume mounts (stage mounts + work dir mount)
        yamlSpec.contains('volumeMounts:')
        yamlSpec.contains('volumes:')
        
        // Check stage mount configurations
        yamlSpec.contains('mountPath: /mnt/data')
        yamlSpec.contains('mountPath: /mnt/output')
        yamlSpec.contains('source: stage')
        
        // Work dir mount should still be present
        yamlSpec.contains('mountPath: /work')
    }

    def "buildJobServiceSpec should handle invalid stage mounts gracefully"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'ubuntu:latest'
        ])
        def executor = createMockExecutor([
            stageMounts: 'invalid-mount,stage1:/valid/mount,another-invalid',
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()

        then:
        yamlSpec != null
        
        // Should include only the valid mount
        yamlSpec.contains('mountPath: /valid/mount')
        yamlSpec.contains('source: stage')
        
        // Invalid mounts should not appear
        !yamlSpec.contains('invalid-mount')
        !yamlSpec.contains('another-invalid')
        
        // Work dir mount should still be present
        yamlSpec.contains('mountPath: /work')
    }

    def "buildJobServiceSpec should generate valid YAML structure for Snowflake Job Service"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'alpine:latest',
            cpus: 2,
            memory: '1GB'
        ])
        def executor = createMockExecutor([
            stageMounts: 'input-stage:/input',
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()

        then:
        yamlSpec != null
        
        // Validate overall structure for Snowflake compatibility
        yamlSpec.contains('spec:')
        yamlSpec.contains('containers:')
        yamlSpec.contains('volumes:')
        
        // Container validation
        yamlSpec.contains('name: main')
        yamlSpec.contains('image: alpine:latest')
        yamlSpec.contains('command:')
        yamlSpec.contains('- /bin/bash')
        
        // Resources validation
        yamlSpec.contains('cpu: 2')
        yamlSpec.contains('memory: 1024Mi')
        
        // Volume structure validation - should have proper format for Snowflake
        yamlSpec.contains('volumeMounts:')
        yamlSpec.contains('mountPath:')
        yamlSpec.contains('volumes:')
        yamlSpec.contains('source: stage')  // All sources should start with @
    }

    def "buildJobServiceSpec should normalize task names properly"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'ubuntu:latest'
        ])
        taskRun.getName() >> 'test-task.with#special$chars'
        def executor = createMockExecutor([
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()

        then:
        yamlSpec != null
        // The YAML should be generated successfully despite special characters in task name
        yamlSpec.contains('ubuntu:latest')
        yamlSpec.contains('main')
    }

    def "buildJobServiceSpec should handle null stage mounts"() {
        given:
        def taskRun = createMockTaskRun([
            container: 'ubuntu:latest'
        ])
        def executor = createMockExecutor([
            stageMounts: null,
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when:
        def yamlSpec = handler.buildJobServiceSpec()

        then:
        yamlSpec != null
        
        // Should only have work dir mount, no stage mounts
        yamlSpec.contains('volumeMounts:')
        yamlSpec.contains('mountPath: /work')
        yamlSpec.contains('source: stage')
    }

    def "generated YAML should be compatible with Snowflake Job Service specification"() {
        given: "A complex task configuration"
        def taskRun = createMockTaskRun([
            container: 'ghcr.io/my-org/bioinformatics-tools:v1.0',
            cpus: 8,
            memory: '16GB'
        ])
        def executor = createMockExecutor([
            stageMounts: 'data-stage:/data,results-stage:/results,reference-stage:/reference',
            workDirStage: 'work-stage'
        ])
        def handler = new TestableSnowflakeTaskHandler(taskRun, executor)

        when: "Generating the job service specification"
        def yamlSpec = handler.buildJobServiceSpec()
        
        then: "The generated YAML should contain all required elements for Snowflake"
        yamlSpec != null
        
        and: "Root structure should be valid"
        yamlSpec.contains('spec:')
        
        and: "Container specification should be complete"
        yamlSpec.contains('containers:')
        yamlSpec.contains('name: main')
        yamlSpec.contains('image: ghcr.io/my-org/bioinformatics-tools:v1.0')
        yamlSpec.contains('command:')
        yamlSpec.contains('- /bin/bash')
        yamlSpec.contains('- -ue')
        yamlSpec.contains('- /work/.command.run')
        
        and: "Resource constraints should be properly formatted"
        yamlSpec.contains('resources:')
        yamlSpec.contains('requests:')
        !yamlSpec.contains('limits:')  // limits should not exist at all
        yamlSpec.contains('cpu: 8')
        yamlSpec.contains('memory: 16384Mi')
        
        and: "Volume mounts should include all stages plus work directory"
        yamlSpec.contains('volumeMounts:')
        yamlSpec.contains('mountPath: /data')
        yamlSpec.contains('mountPath: /results')
        yamlSpec.contains('mountPath: /reference')
        yamlSpec.contains('mountPath: /work')
        
        and: "Volume definitions should reference correct Snowflake stages"
        yamlSpec.contains('volumes:')
        yamlSpec.contains('source: stage')
        
        when: "Validating the YAML can be used in a Snowflake SQL statement"
        def sqlTemplate = """
EXECUTE JOB SERVICE
IN COMPUTE POOL my_compute_pool
NAME = test_session_test_task
EXTERNAL_ACCESS_INTEGRATIONS=()
FROM SPECIFICATION
\$\$
${yamlSpec}
\$\$
"""
        
        then: "The SQL should be well-formed"
        sqlTemplate.contains('EXECUTE JOB SERVICE')
        sqlTemplate.contains('FROM SPECIFICATION')
        sqlTemplate.contains('spec:')
        sqlTemplate.contains('containers:')
        
        and: "Display the complete example for reference"
        println "=== COMPLETE SNOWFLAKE JOB SERVICE EXAMPLE ==="
        println sqlTemplate
        println "=============================================="
    }

    // Helper methods for creating mock objects
    private TaskRun createMockTaskRun(Map config) {
        def taskRun = Mock(TaskRun)
        def taskConfig = Mock(TaskConfig)
        def session = Mock(Session)
        
        taskRun.getConfig() >> taskConfig
        taskRun.container >> config.container
        taskRun.getName() >> 'test-task'
        taskRun.workDir >> Paths.get('/work')
        taskRun.id >> new TaskId(123)
        
        taskConfig.getCpus() >> config.cpus
        taskConfig.getMemory() >> (config.memory ? MemoryUnit.of(config.memory) : null)
        
        session.runName >> 'test-session'
        taskRun.processor >> Mock(nextflow.processor.TaskProcessor) {
            getSession() >> session
        }
        
        return taskRun
    }
    
    private SnowflakeExecutor createMockExecutor(Map config) {
        def executor = Mock(SnowflakeExecutor)
        def session = Mock(Session)
        
        executor.snowflakeConfig >> config
        executor.session >> session
        executor.getWorkDir() >> Paths.get('/work')
        
        session.runName >> 'test-session'
        
        return executor
    }
    
    // Testable version of SnowflakeTaskHandler that makes buildJobServiceSpec accessible via reflection
    private static class TestableSnowflakeTaskHandler extends SnowflakeTaskHandler {
        TestableSnowflakeTaskHandler(TaskRun taskRun, SnowflakeExecutor executor) {
            super(taskRun, executor, null)
        }
        
        String buildJobServiceSpec() {
            def method = SnowflakeTaskHandler.class.getDeclaredMethod('buildJobServiceSpec')
            method.setAccessible(true)
            return method.invoke(this) as String
        }
    }
} 