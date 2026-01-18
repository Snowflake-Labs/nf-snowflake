package nextflow.snowflake

import groovy.transform.CompileStatic
import nextflow.processor.TaskBean
import nextflow.executor.SimpleFileCopyStrategy
import java.nio.file.Path

/**
 * File copy strategy for Snowflake executor with remote bin directory support
 * 
 * @author Hongye Yu
 */
@CompileStatic
class SnowflakeFileCopyStrategy extends SimpleFileCopyStrategy {
    
    private Path remoteBinDir
    
    SnowflakeFileCopyStrategy(TaskBean bean, SnowflakeExecutor executor) {
        super(bean)
        this.remoteBinDir = executor.getRemoteBinDir()
    }
    
    /**
     * Override to prepend remote bin directory setup script
     */
    @Override
    String getEnvScript(Map environment, boolean container) {
        if( remoteBinDir == null )
            return super.getEnvScript(environment, container)
        
        final script = """\
            # Copy and setup remote bin directory
            NXF_BIN=\$(mktemp -d)
            cp -r ${remoteBinDir}/* \$NXF_BIN/
            chmod +x \$NXF_BIN/*
            export PATH=\$NXF_BIN:\$PATH
            """.stripIndent()
        
        return script + super.getEnvScript(environment, container)
    }
}

