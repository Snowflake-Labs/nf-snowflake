package nextflow.snowflake

import groovy.transform.CompileStatic
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskRun
import nextflow.util.Escape

@CompileStatic
class SnowflakeWrapperBuilder extends BashWrapperBuilder {

    SnowflakeWrapperBuilder(TaskRun task) {
        super(task)
        this.headerScript = "NXF_CHDIR=${Escape.path(task.workDir)}"
    }
}
