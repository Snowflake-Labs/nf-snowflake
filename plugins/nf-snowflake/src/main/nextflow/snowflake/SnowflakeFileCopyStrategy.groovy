package nextflow.snowflake

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.processor.TaskBean
import nextflow.executor.SimpleFileCopyStrategy
import java.nio.file.Path

/**
 * File copy strategy for Snowflake executor with remote bin directory support
 * 
 * Note: Path translation is handled by SnowflakeWrapperBuilder.create() which
 * pre-translates all paths in the TaskBean before this strategy is created.
 * This class only needs to handle bin directory setup.
 * 
 * @author Hongye Yu
 */
@Slf4j
@CompileStatic
class SnowflakeFileCopyStrategy extends SimpleFileCopyStrategy {
    
    private Path remoteBinDir
    
    SnowflakeFileCopyStrategy(TaskBean bean, SnowflakeExecutor executor) {
        super(bean)
        this.remoteBinDir = executor.getRemoteBinDir()
    }
    
    /**
     * Override to prepend remote bin directory setup script
     * 
     * The remoteBinDir is a snowflake:// path, so we need to translate it
     * to the container mount path for the script.
     */
    @Override
    String getEnvScript(Map environment, boolean container) {
        if( remoteBinDir == null ) {
            log.debug("No remote bin directory configured")
            return super.getEnvScript(environment, container)
        }
        
        // Translate bin directory path from snowflake:// to /mnt/stage/...
        String translatedBinDir = SnowflakeUri.translateToMount(remoteBinDir).toUriString()
        log.debug("Remote bin directory: ${remoteBinDir} -> ${translatedBinDir}")
        
        final script = """\
            # Copy and setup remote bin directory
            NXF_BIN=\$(mktemp -d)
            cp -r ${translatedBinDir}/* \$NXF_BIN/ 2>&1 || echo "[DEBUG] Failed to copy bin files" >&2
            chmod +x \$NXF_BIN/* 2>&1
            export PATH=\$NXF_BIN:\$PATH
            """.stripIndent()
        
        // Get parent env script and handle null return value
        def parentScript = super.getEnvScript(environment, container)
        return script + (parentScript ?: '')
    }
}

